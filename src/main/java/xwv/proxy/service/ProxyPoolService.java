package xwv.proxy.service;

import okhttp3.Dns;
import org.apache.http.HttpHost;
import redis.clients.jedis.JedisPoolConfig;
import xwv.crawler.SimpleCrawler;
import xwv.crawler._66daili;
import xwv.crawler.kuaidaili;
import xwv.crawler.xicidaili;
import xwv.jedis.AutoCloseJedisPool;
import xwv.proxy.Proxy;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProxyPoolService {
    public interface TestCase {
        boolean test(Proxy proxy);
    }

    private static ProxyPoolService instance;
    private static Lock staticLock = new ReentrantLock(true);

    public static ProxyPoolService getInstance() {
        ProxyPoolService result = initInstance(null, null);
        return result != null ? result : instance;
    }

    public static ProxyPoolService initInstance(TestCase testCase) {
        return initInstance(null, testCase);
    }


    public static ProxyPoolService initInstance(Dns dns) {
        return initInstance(dns, null);
    }

//    private static class Instance {
//        private static final ProxyPoolService instance = new ProxyPoolService();
//    }

    public static ProxyPoolService initInstance(Dns dns, TestCase testCase) {
        staticLock.lock();
        try {
            if (instance == null) {
                instance = new ProxyPoolService(dns, testCase);
                return instance;
            } else {
//                System.err.println("[ERROR]ProxyPoolService:Init has been ran");
                return null;
            }
        } finally {
            staticLock.unlock();
        }
    }


    private Dns dns = null;
    private TestCase testCase = null;

    private final Lock lock = new ReentrantLock(true);
    private final ConcurrentMap<String, Proxy> proxyMap = new ConcurrentHashMap<String, Proxy>();


    private AutoCloseJedisPool jedis = new AutoCloseJedisPool(new JedisPoolConfig() {{
        setTestOnBorrow(true);
    }}, "127.0.0.1");

    private Proxy last;
    private ThreadPoolExecutor initExecutor;

    private ProxyPoolService(Dns dns, TestCase testCase) {
        if (testCase != null) {
            this.testCase = testCase;
        } else {
            this.testCase = new TestCase() {
                @Override
                public boolean test(Proxy proxy) {
                    if (proxy.Post("http://wapp.baidu.com", false) != null) {
                        if (proxy.Post("https://www.baidu.com", false) != null) {
                            return true;
                        }
                    }
                    return false;
                }
            };
        }
        this.dns = dns;

        ThreadPoolExecutor service = new ThreadPoolExecutor(8, 8,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        initExecutor = service;

        Runnable init_runnable = new Runnable() {
            private volatile int index;

            @Override
            public void run() {
                long size = jedis.call(jedis -> jedis.zcard("proxy"));
                Set<String> proxy = jedis.call(jedis -> jedis.zrevrange("proxy", 0, size));
                if (proxy != null) {
                    System.out.println(proxy.size());
                    for (String p : proxy) {
                        service.execute(new Runnable() {
                            @Override
                            public void run() {
                                if (index % 30 == 0) {
                                    System.out.println(index++);
                                }
                                put(p);
                            }
                        });
                    }
                }
            }
        };

        Runnable runnable = new Runnable() {


            @Override
            public void run() {
                long time = System.currentTimeMillis();
                Set<String> proxy = jedis.call(jedis ->
                        jedis.zrevrangeByScore(
                                "proxy",
                                time / 3600000d,
                                (time - 30 * 60 * 1000) / 3600000d
                        ));
                if (proxy != null) {
                    for (String p : proxy) {
                        service.execute(new Runnable() {
                            @Override
                            public void run() {

                                put(p);
                            }
                        });
                    }
                }
            }
        };


        service.execute(new Runnable() {
            @Override
            public void run() {
                boolean flag = true;
                while (true) {
                    if (service.getActiveCount() < 4) {
                        if (flag) {
                            flag = false;
                            service.execute(init_runnable);
                        } else {
                            service.execute(runnable);
                        }

                    }
                    try {
                        Thread.sleep(5 * 60 * 1000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }

            }
        });


    }


    public boolean put(String proxyStr, TestCase testCase) {
        try {
            String[] arr = proxyStr.split(":");
            if (arr.length > 1) {
                Proxy proxy = new Proxy(arr[0], Integer.parseInt(arr[1]), dns);

                if (!has(proxy.getHost(), proxy.getPort())) {
                    if (testCase == null || testCase.test(proxy)) {
                        put(proxy);
                        return true;
                    }
                }
            }
        } catch (Exception ignored) {

        }
        return false;
    }


    public boolean put(String proxyStr) {
        return put(proxyStr, testCase);
    }

    private void put(Proxy proxy) {
        try {
            proxyMap.put(proxy.toString(), proxy);
            jedis.call(jedis -> jedis.zadd(
                    "proxy",
                    System.currentTimeMillis() / 3600000d,
                    proxy.toString()
            ));
            System.out.println("Add Proxy:" + proxy);
            Count();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public boolean has(String host, int port) {
        String key = host + ":" + port;
        lock.lock();
        try {
            return proxyMap.get(key) != null;
        } finally {
            lock.unlock();
        }
    }


    public void refresh(String proxy) {
        jedis.call(jedis -> jedis.zadd("proxy", System.currentTimeMillis() / 3600000d, proxy));
    }

    public void refresh(Proxy proxy) {
        refresh(proxy);
    }

    public void remove(String host, int port) {
        remove(host + ":" + port);
    }

    public void remove(String key) {
        remove(key, (long) 60l);
    }

    public void remove(String key, long sec) {
        lock.lock();

        try {
            Proxy proxy;
            if ((proxy = proxyMap.get(key)) != null) {
                proxy.disable(sec);
            }

        } finally {
            lock.unlock();
            Count();
        }
    }

    private volatile int count = 0;

    public int getCount() {
        return count;
    }

    public void Count() {


        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                int count = 0;
                for (String key : proxyMap.keySet()) {
                    Proxy proxy;
                    if ((proxy = proxyMap.get(key)) != null && !proxy.isNull() && proxy.isEnable()) {
                        count++;
                    }
                }
                ProxyPoolService.this.count = count;
                System.out.println(/*ProxyPoolService.this + " " +*/ "Proxy Count:" + count);
            }
        };
        if (executor != null && !executor.isShutdown()) {
            executor.execute(runnable);
        } else {
            runnable.run();
        }

    }

    private Iterator<String> iterator;

    public Proxy getNext() {
        lock.lock();
        try {
            Proxy proxy = null;
            String last = null;

            Set<String> keySet = proxyMap.keySet();


            while (proxy == null || proxy.isNull() || !proxy.isEnable()) {
                if (keySet.size() < 1) {
                    return null;
                }
                if (iterator == null || !iterator.hasNext()) {
                    iterator = keySet.iterator();
                }
                String key = iterator.next();


                proxy = proxyMap.get(key);
                if (last != null) {
                    if (last.equals(key) || keySet.size() < 2) {
                        break;
                    }
                } else {
                    last = key;
                }
            }
            if (proxy == this.last) {
                Thread.sleep(200);
            }
            this.last = proxy;
            if (proxy.isEnable()) {
                return proxy;
            } else {
                return null;
            }
        } catch (InterruptedException e) {
            System.out.println(e);
            return null;
        } finally {
            lock.unlock();
        }
    }

    public java.net.Proxy getNextProxy(java.net.Proxy.Type type) {
        Proxy proxyInfo = getNext();
        if (proxyInfo == null) {
            return null;
        }

        String host = proxyInfo.getHost();
        if (host == null || host.isEmpty()) {
            return null;
        }
        return new java.net.Proxy(type, new InetSocketAddress(host, proxyInfo.getPort()));
    }

    public HttpHost getNextHost() {
        Proxy proxyInfo = getNext();
        if (proxyInfo == null) {
            return null;
        }

        String host = proxyInfo.getHost();
        if (host == null || host.isEmpty()) {
            return null;
        }
        return new HttpHost(host, proxyInfo.getPort());
    }

//    public boolean testProxy(String host, int port) {
//        return !has(host, port) && HttpUtil.Test("http://300report.jumpw.com/api/getrank?type=0", new java.net.Proxy(java.net.Proxy.Type.HTTP, new InetSocketAddress(host, port)), 1000) && HttpUtil.Test("http://300report.jumpw.com/api/getrank?type=1", new java.net.Proxy(java.net.Proxy.Type.HTTP, new InetSocketAddress(host, port)), 1000) && HttpUtil.Test("http://300report.jumpw.com/api/getrank?type=2", new java.net.Proxy(java.net.Proxy.Type.HTTP, new InetSocketAddress(host, port)), 1000);
//    }


    private ExecutorService executor;

    public ExecutorService getExecutor() {
        if (executor == null || executor.isShutdown()) {
            executor = Executors.newCachedThreadPool();

        }
        return executor;
    }


    public void start() {
        if (!isRunning()) {
            startCrawler(new kuaidaili());
            startCrawler(new kuaidaili("inha"));
            startCrawler(new xicidaili());
            startCrawler(new xicidaili("nt"));
            startCrawler(new xicidaili("wn"));
            startCrawler(new xicidaili("wt"));
            startCrawler(new _66daili());
            getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        Count();
                        try {
                            Thread.sleep(5 * 60 * 1000);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            });
        }
    }

    public void stop() {
        getExecutor().shutdownNow();
        if (initExecutor != null) {
            initExecutor.shutdownNow();
        }
    }

    public boolean isRunning() {
        return executor != null && !executor.isShutdown();
    }


    private void startCrawler(SimpleCrawler crawler) {
        getExecutor().execute(new Runnable() {
            @Override
            public void run() {

                while (crawler.hasNextUrlQueue()) {
                    for (String url : crawler.nextUrlQueue()) {
                        while (crawler.getExecutor().getQueue().size() > 10) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                break;
                            }
                        }
                        crawler.getExecutor().execute(new Runnable() {
                            @Override
                            public void run() {
                                crawler.parseOne(url);
                            }
                        });
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            System.out.println(e);
                        }
                    }

                }
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
//        Jedis jedis = new Jedis("127.0.0.1");
//        Set<String> members = jedis.smembers("proxy");
//
//        jedis.del("proxy");
//        for (String s : members) {
//            jedis.zadd("proxy", 0, s);
//            jedis.zrangeWithScores()
//        }


//        ProxyPoolService.getInstance().start();
//        while (true) {
////            System.out.println("Proxy:" + ProxyPoolService.getInstance().getNext());
//            Thread.sleep(1000);
//        }

    }
}
