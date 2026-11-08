using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using Lidgren.Network;
using NUnit.Framework;

namespace UnitTests;

// CMU14: drive real UDP peers synchronously to observe receive ordering within one heartbeat.
[TestFixture]
public sealed class NetPeerReceiveInterleaveTests
{
	[TestCase(0, 3)]
	[TestCase(1, 0)]
	public void QueuedInputIsReleasedBeforeOutgoingConnectionBurst(int interval, int expectedSends)
	{
		using var peers = new ConnectedPeers(interval);
		var received = new List<(int Value, long Sends)>();
		var sentBefore = peers.Server.Statistics.SentPackets;
		peers.Server.RegisterReceivedCallback(_ =>
		{
			foreach (var value in ReadValues(peers.Server))
				received.Add((value, peers.Server.Statistics.SentPackets - sentBefore));
		}, new InlineContext());

		QueueMessage(peers.Clients[0], peers.Clients[0].Connections[0], 42);
		SetField(peers.Clients[0], "m_frameCounter", 0u);
		Pulse(peers.Clients[0]);
		Assert.That(peers.Server.Socket!.Poll(1_000_000, System.Net.Sockets.SelectMode.SelectRead), Is.True);
		peers.QueueServerBurst();
		SetField(peers.Server, "m_frameCounter", 0u); // Avoid periodic ACK/ping traffic in the observed heartbeat.
		Pulse(peers.Server);

		Assert.That(received, Is.EqualTo(new[] { (42, (long) expectedSends) }),
			"Input already queued should be released before sending when enabled, or after the full burst when disabled.");
		peers.AssertServerBurstArrived();
	}

	[TestCase(2, 4 * 1024 * 1024)]
	[TestCase(1024, 500)]
	public void InterleavedReadsShareTheHeartbeatReceiveBudget(int packetBudget, int byteBudget)
	{
		using var peers = new ConnectedPeers(1, packetBudget, byteBudget);
		for (var i = 0; i < 5; i++)
			QueueMessage(peers.Clients[0], peers.Clients[0].Connections[0], i);
		SetField(peers.Clients[0], "m_frameCounter", 0u);
		Pulse(peers.Clients[0]);
		Assert.That(peers.Server.Socket!.Poll(1_000_000, System.Net.Sockets.SelectMode.SelectRead), Is.True);
		peers.QueueServerBurst();
		SetField(peers.Server, "m_frameCounter", 0u);
		Pulse(peers.Server);

		Assert.That(ReadValues(peers.Server), Is.EqualTo(new[] { 0, 1 }),
			"Extra receive checks must not reset the packet or byte budget and starve outgoing work.");
		Pulse(peers.Server);
		Assert.That(ReadValues(peers.Server), Is.EqualTo(new[] { 2, 3 }));
		Pulse(peers.Server);
		Assert.That(ReadValues(peers.Server), Is.EqualTo(new[] { 4 }),
			"Messages left in the UDP socket must remain ordered and be processed on following heartbeats.");
		peers.AssertServerBurstArrived();
	}

	[TestCase(0, 2u, 1)]
	[TestCase(1, 2u, 0)]
	[TestCase(1, 0u, 0)]
	public void QueuedAcknowledgementIsReadBeforeOverdueRetransmission(int interval, uint frameCounter, int expectedResends)
	{
		using var peers = new ConnectedPeers(interval);
		var connection = peers.Server.Connections[0];
		var client = peers.Clients.Single(c => c.Socket!.LocalEndPoint!.Equals(connection.RemoteEndPoint));
		QueueMessage(peers.Server, connection, 91);
		SetField(peers.Server, "m_frameCounter", 0u);
		Pulse(peers.Server);
		Pulse(client);
		Assert.That(ReadValues(client), Is.EqualTo(new[] { 91 }));
		SetField(client, "m_frameCounter", 2u);
		Pulse(client); // Put the acknowledgement in the server socket without servicing it yet.
		Assert.That(peers.Server.Socket!.Poll(1_000_000, System.Net.Sockets.SelectMode.SelectRead), Is.True);

		// Make the already-acknowledged packet eligible for the timer resend without a timing-sensitive sleep.
		var channels = (Array) typeof(NetConnection).GetField("m_sendChannels", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(connection)!;
		var channel = channels.Cast<object?>().Single(c => c != null)!;
		channel.GetType().GetField("m_resendDelay", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(channel, 0d);
		var resends = connection.Statistics.ResentMessages;
		SetField(peers.Server, "m_frameCounter", frameCounter);
		Pulse(peers.Server);

		Assert.That(connection.Statistics.ResentMessages - resends, Is.EqualTo(expectedResends),
			"An ACK already queued at heartbeat entry should retire the reliable message before the resend scan.");
		Pulse(client);
		Assert.That(ReadValues(client), Is.Empty, "A reliable retransmission must never duplicate application delivery.");
	}

	private static void QueueMessage(NetPeer peer, NetConnection connection, int value)
	{
		var message = peer.CreateMessage();
		message.Write(value);
		message.Write(Enumerable.Repeat((byte) value, 400).ToArray()); // Below the existing 508-byte MTU.
		peer.SendMessage(message, connection, NetDeliveryMethod.ReliableOrdered);
	}

	private static List<int> ReadValues(NetPeer peer)
	{
		var values = new List<int>();
		while (peer.ReadMessage() is { } message)
		{
			if (message.MessageType == NetIncomingMessageType.Data)
			{
				var value = message.ReadInt32();
				values.Add(value);
				Assert.That(message.ReadBytes(400), Is.All.EqualTo((byte) value), "Interleaving must not corrupt the shared send buffer.");
			}
			peer.Recycle(message);
		}
		return values;
	}

	private static void SetField(NetPeer peer, string name, object value)
		=> typeof(NetPeer).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(peer, value);

	private static void Invoke(NetPeer peer, string name)
		=> typeof(NetPeer).GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(peer, null);

	private static void Pulse(NetPeer peer)
	{
		SetField(peer, "m_lastHeartbeat", -1d);
		Invoke(peer, "Heartbeat");
	}

	private static void Initialize(NetPeer peer)
	{
		SetField(peer, "m_networkThread", Thread.CurrentThread);
		Invoke(peer, "InitializeNetwork");
	}

	[Test]
	public void JoiningDuringSendTraversalPreservesExistingConnections()
	{
		using var peers = new ConnectedPeers(1);
		var joining = new NetClient(new NetPeerConfiguration(nameof(NetPeerReceiveInterleaveTests))
		{
			LocalAddress = IPAddress.Loopback,
		});
		try
		{
			Initialize(joining);
			joining.Connect((IPEndPoint) peers.Server.Socket!.LocalEndPoint!);
			SetField(joining, "m_frameCounter", 2u); // Handshake attempts run every third heartbeat.
			Pulse(joining);
			peers.QueueServerBurst();
			Pulse(peers.Server);
			peers.AssertServerBurstArrived();
			Pulse(joining); // Read ConnectResponse and send ConnectionEstablished back to the server.
			ReadValues(joining);

			peers.QueueServerBurst();
			Pulse(peers.Server); // The interleaved receive appends the newly established connection.
			Assert.That(peers.Server.ConnectionsCount, Is.EqualTo(4));
			peers.AssertServerBurstArrived();

			peers.QueueServerBurst();
			Pulse(peers.Server);
			peers.AssertServerBurstArrived();
			Pulse(joining);
			Assert.That(ReadValues(joining), Is.EqualTo(new[] { 73 }));
		}
		finally
		{
			joining.Socket?.Dispose();
		}
	}

	private sealed class InlineContext : SynchronizationContext
	{
		public override void Post(SendOrPostCallback callback, object? state) => callback(state);
	}

	private sealed class ConnectedPeers : IDisposable
	{
		public NetServer Server { get; }
		public List<NetClient> Clients { get; } = new();

		public ConnectedPeers(int interval, int packetBudget = 1024, int byteBudget = 4 * 1024 * 1024)
		{
			Server = new NetServer(new NetPeerConfiguration(nameof(NetPeerReceiveInterleaveTests))
			{
				LocalAddress = IPAddress.Loopback,
				MaximumIpConnections = 4,
				MaximumRapidConnections = 10,
				ReceiveInterleavePacketInterval = interval,
				MaximumPacketsPerHeartbeat = packetBudget,
				MaximumBytesPerHeartbeat = byteBudget,
			});
			try
			{
				Initialize(Server);
				for (var i = 0; i < 3; i++)
				{
					var client = new NetClient(new NetPeerConfiguration(nameof(NetPeerReceiveInterleaveTests))
					{
						LocalAddress = IPAddress.Loopback,
					});
					Clients.Add(client);
					Initialize(client);
					client.Connect((IPEndPoint) Server.Socket!.LocalEndPoint!);
				}
				var deadline = Environment.TickCount64 + 5000;
				while (Server.ConnectionsCount != 3 || Clients.Any(c => c.ConnectionStatus != NetConnectionStatus.Connected))
				{
					if (Environment.TickCount64 > deadline)
						Assert.Fail("UDP peers failed to connect.");
					foreach (var client in Clients) Pulse(client);
					Pulse(Server);
					ReadValues(Server);
					foreach (var client in Clients) ReadValues(client);
					Thread.Sleep(1);
				}
				// Finish handshake/ping replies before measuring the deliberately tiny data receive budgets.
				for (var i = 0; i < 6; i++)
				{
					foreach (var client in Clients) Pulse(client);
					Pulse(Server);
				}
				ReadValues(Server);
				foreach (var client in Clients) ReadValues(client);
			}
			catch
			{
				Dispose();
				throw;
			}
		}

		public void QueueServerBurst()
		{
			foreach (var connection in Server.Connections)
				QueueMessage(Server, connection, 73);
		}

		public void AssertServerBurstArrived()
		{
			foreach (var client in Clients)
			{
				Pulse(client);
				Assert.That(ReadValues(client), Is.EqualTo(new[] { 73 }), "Every connection must still receive its outgoing message.");
			}
		}

		public void Dispose()
		{
			foreach (var client in Clients) client.Socket?.Dispose();
			Server.Socket?.Dispose();
		}
	}
}
