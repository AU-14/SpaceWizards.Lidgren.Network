using System;
using System.Reflection;
// CMU14: concurrency regression helpers.
using System.Linq;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Lidgren.Network;
using NUnit.Framework;

namespace UnitTests;

[TestFixture]
[TestOf(typeof(NetPeer))]
public sealed class NetPeerStoragePoolTests
{
	[Test]
	public void GetStorageRoundsSmallRequestsUpToBucketSize()
	{
		var peer = CreatePeer();

		var storage = peer.GetStorage(17);

		Assert.That(storage.Length, Is.EqualTo(32));
	}

	[Test]
	public void RecycleReusesMatchingBucket()
	{
		var peer = CreatePeer();
		var storage = peer.GetStorage(17);

		peer.Recycle(storage);
		var reused = peer.GetStorage(31);

		Assert.Multiple(() =>
		{
			Assert.That(reused, Is.SameAs(storage));
			Assert.That(reused.Length, Is.EqualTo(32));
			Assert.That(peer.Statistics.BytesInRecyclePool, Is.Zero);
			Assert.That(peer.m_storageSlotsUsedCount, Is.Zero);
		});
	}

	[Test]
	public void GetStorageDoesNotReuseSmallerBucketForLargerRequest()
	{
		var peer = CreatePeer();
		var storage = peer.GetStorage(17);
		peer.Recycle(storage);

		var larger = peer.GetStorage(33);

		Assert.Multiple(() =>
		{
			Assert.That(larger, Is.Not.SameAs(storage));
			Assert.That(larger.Length, Is.EqualTo(64));
			Assert.That(peer.Statistics.BytesInRecyclePool, Is.EqualTo(32));
			Assert.That(peer.m_storageSlotsUsedCount, Is.EqualTo(1));
		});
	}

	[Test]
	public void RecycleHonorsMaxCacheCount()
	{
		var peer = CreatePeer(configuration => configuration.RecycledCacheMaxCount = 1);
		var first = peer.GetStorage(17);
		var second = peer.GetStorage(33);

		peer.Recycle(first);
		peer.Recycle(second);

		Assert.Multiple(() =>
		{
			Assert.That(peer.Statistics.BytesInRecyclePool, Is.EqualTo(32));
			Assert.That(peer.m_storageSlotsUsedCount, Is.EqualTo(1));
		});
	}

	[Test]
	public void RecycleIgnoresOversizedStorage()
	{
		var peer = CreatePeer();
		var storage = peer.GetStorage((1024 * 1024) + 1);

		peer.Recycle(storage);

		Assert.Multiple(() =>
		{
			Assert.That(storage.Length, Is.EqualTo((1024 * 1024) + 1));
			Assert.That(peer.Statistics.BytesInRecyclePool, Is.Zero);
			Assert.That(peer.m_storageSlotsUsedCount, Is.Zero);
		});
	}

	[Test]
	public void RecyclingDisabledAllocatesExactStorageAndDoesNotPool()
	{
		var peer = CreatePeer(configuration => configuration.UseMessageRecycling = false);
		var storage = peer.GetStorage(17);

		peer.Recycle(storage);

		Assert.Multiple(() =>
		{
			Assert.That(storage.Length, Is.EqualTo(17));
			Assert.That(peer.Statistics.BytesInRecyclePool, Is.Zero);
			Assert.That(peer.m_storageSlotsUsedCount, Is.Zero);
		});
	}

	// CMU14: send workers and the receive thread share the bounded storage cache.
	[Test]
	public void ConcurrentRentAndRecyclePreservesOwnershipAndCapacity()
	{
		const int limit = 4;
		var peer = CreatePeer(configuration => configuration.RecycledCacheMaxCount = limit);
		var leased = new ConcurrentDictionary<byte[], int>();
		using var start = new ManualResetEventSlim();
		var workers = Enumerable.Range(0, 8).Select(worker => Task.Factory.StartNew(() =>
		{
			start.Wait();
			for (var iteration = 0; iteration < 2000; iteration++)
			{
				var minimum = 17 << ((worker + iteration) % 4);
				var storage = peer.GetStorage(minimum);
				if (storage.Length < minimum || !leased.TryAdd(storage, worker))
					throw new InvalidOperationException("Storage is undersized or already leased to another worker.");
				storage[0] = (byte) worker;
				if ((iteration & 31) == 0)
					Thread.Yield();
				if (storage[0] != worker || !leased.TryRemove(storage, out _))
					throw new InvalidOperationException("Another worker changed an outstanding message buffer.");
				peer.Recycle(storage);
			}
		}, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default)).ToArray();
		start.Set();
		Task.WaitAll(workers);

		var retained = peer.m_storagePools!.SelectMany(pool => pool.ToArray()).ToArray();
		Assert.Multiple(() =>
		{
			Assert.That(retained.Length, Is.LessThanOrEqualTo(limit), "Concurrent returns must honor the global cache limit.");
			Assert.That(retained.Distinct().Count(), Is.EqualTo(retained.Length), "A retained buffer must have only one owner.");
			Assert.That(peer.m_storageSlotsUsedCount, Is.EqualTo(retained.Length));
			Assert.That(peer.Statistics.BytesInRecyclePool, Is.EqualTo(retained.Sum(storage => storage.Length)));
		});
	}

	private static NetPeer CreatePeer(Action<NetPeerConfiguration>? configure = null)
	{
		var configuration = new NetPeerConfiguration(nameof(NetPeerStoragePoolTests));
		configure?.Invoke(configuration);
		var peer = new NetPeer(configuration);
		InitializePools(peer);
		return peer;
	}

	private static void InitializePools(NetPeer peer)
	{
		var method = typeof(NetPeer).GetMethod("InitializePools", BindingFlags.Instance | BindingFlags.NonPublic);
		Assert.That(method, Is.Not.Null, "NetPeer.InitializePools is missing.");
		method!.Invoke(peer, Array.Empty<object>());
	}
}
