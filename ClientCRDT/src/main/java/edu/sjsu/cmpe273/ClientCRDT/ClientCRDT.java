package edu.sjsu.cmpe273.ClientCRDT;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientCRDT 
{
	private List<DistributedCacheService> serverList;
	private CountDownLatch latch;

	public ClientCRDT() {
		DistributedCacheService cache1 = new DistributedCacheService(
				"http://localhost:3000");
		DistributedCacheService cache2 = new DistributedCacheService(
				"http://localhost:3001");
		DistributedCacheService cache3 = new DistributedCacheService(
				"http://localhost:3002");

		this.serverList = new ArrayList<DistributedCacheService>();

		serverList.add(cache1);
		serverList.add(cache2);
		serverList.add(cache3);
	}

	public String getKeyForValue(Map<String, Integer> m, int v) 
	{
		for (Entry<String, Integer> e : m.entrySet()) {
			if (v == e.getValue()) return e.getKey();
		}

		return null;
	}

	public boolean put(long key, String value) throws InterruptedException, IOException 
	{
		final AtomicInteger myCount = new AtomicInteger(0);
		this.latch = new CountDownLatch(serverList.size());
		final ArrayList<DistributedCacheService> writeList = new ArrayList<DistributedCacheService>(3);
		for (final DistributedCacheService s : serverList) 
		{
			Future<HttpResponse<JsonNode>> ftr = Unirest.put(s.getCacheServerUrl()+ "/cache/{key}/{value}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.routeParam("value", value)
					.asJsonAsync(new Callback<JsonNode>() {

						public void failed(UnirestException e) {
							latch.countDown();
						}

						public void completed(HttpResponse<JsonNode> response) {
							int count = myCount.incrementAndGet();
							writeList.add(s);
							latch.countDown();
						}

						public void cancelled() {
							latch.countDown();
						}

					});
		}
		this.latch.await();
		if (!(myCount.intValue() > 1)) 
				{
			this.latch = new CountDownLatch(writeList.size());
			for (final DistributedCacheService s : writeList) {
				Future<HttpResponse<JsonNode>> ftr = Unirest.get(s.getCacheServerUrl() + "/cache/{key}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.asJsonAsync(new Callback<JsonNode>() {

							public void failed(UnirestException e) {
								latch.countDown();
							}

							public void completed(HttpResponse<JsonNode> response) {
								latch.countDown();
							}

							public void cancelled() {
								latch.countDown();
							}
						});
			}
			this.latch.await(3, TimeUnit.SECONDS);
			Unirest.shutdown();
			return false;
				} 
		else 
		{
			return true;
		}
	}


	public String get(long key) throws InterruptedException, UnirestException, IOException 
	{
		this.latch = new CountDownLatch(serverList.size());
		final Map<DistributedCacheService, String> r = new HashMap<DistributedCacheService, String>();
		for (final DistributedCacheService s : serverList) {
			Future<HttpResponse<JsonNode>> ftr = Unirest.get(s.getCacheServerUrl() + "/cache/{key}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.asJsonAsync(new Callback<JsonNode>() {

						public void failed(UnirestException e) {
							latch.countDown();
						}

						public void completed(HttpResponse<JsonNode> response) {
							r.put(s, response.getBody().getObject().getString("value"));
							latch.countDown();
						}

						public void cancelled() {
							latch.countDown();
						}
					});
		}
		this.latch.await(3, TimeUnit.SECONDS);
		final Map<String, Integer> map = new HashMap<String, Integer>();
		int maxCount = 0;
		for (String value : r.values()) {
			int count = 1;
			if (map.containsKey(value)) {
				count = map.get(value);
				count++;
			}
			if (maxCount < count)
				maxCount = count;
			map.put(value, count);
		}
		String value = this.getKeyForValue(map, maxCount);
		if (maxCount != this.serverList.size()) {
			for (Entry<DistributedCacheService, String> sData : r.entrySet()) {
				if (!value.equals(sData.getValue())) {
					System.out.println("Repairing "+sData.getKey());
					HttpResponse<JsonNode> response = Unirest.put(sData.getKey() + "/cache/{key}/{value}")
							.header("accept", "application/json")
							.routeParam("key", Long.toString(key))
							.routeParam("value", value)
							.asJson();
				}
			}
			for (DistributedCacheService s : this.serverList) {
				if (r.containsKey(s)) continue;
				System.out.println("Repairing "+s.getCacheServerUrl());
				HttpResponse<JsonNode> response = Unirest.put(s.getCacheServerUrl() + "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.routeParam("value", value)
						.asJson();
			}
		} 
		
		Unirest.shutdown();
		return value;
	}
}