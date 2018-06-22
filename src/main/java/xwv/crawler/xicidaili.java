package xwv.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import xwv.proxy.Proxy;
import xwv.proxy.service.ProxyPoolService;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class xicidaili extends SimpleCrawler {

    private int page = 0;
    private boolean hasNextUrlQueue = true;
    private static final Lock lock = new ReentrantLock(true);
    private volatile int count = 0;
    private String mode = "nn";

    public xicidaili(String mode) {
        if (mode != null && !mode.isEmpty()) {
            this.mode = mode;
        }
    }

    public xicidaili() {
    }

    @Override
    public ConcurrentLinkedQueue<String> nextUrlQueue() {
        page++;
        return new ConcurrentLinkedQueue<String>() {{
            offer("http://www.xicidaili.com/" + mode + "/" + page);
        }};
    }

    @Override
    public boolean hasNextUrlQueue() {
        return hasNextUrlQueue;
    }

    @Override
    public void reset() {
        page = 0;
        hasNextUrlQueue = true;
    }

    @Override
    public void parseOne(String url) {
        ProxyPoolService service = ProxyPoolService.getInstance();

        try {
            Document document = parse(url);
            if (document == null) {
                document = parse(url);
            }
            if (document == null) {
                return;
            }

            Elements pagination = document.selectFirst(".pagination").children();
            int size = pagination.size();
            int maxPage = Integer.parseInt(pagination.get(size - 2)
                    .text());
            if (page >= maxPage) {
                page = 0;
            }
            Elements elements = document.select("#ip_list").select("tbody tr");
            for (Element e : elements) {
                if (e.child(1).tagName().equals("th")) {
                    continue;
                }
                String host = e.child(1).text();
                int port = Integer.parseInt(e.child(2).text());

                Proxy proxyInfo = new Proxy(host, port);
                getExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        service.put(proxyInfo.toString());
                    }
                });


            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Document parse(String url) throws IOException {
//        System.out.println(Thread.currentThread().getName());
        lock.lock();
        try {
//            System.out.println("parse:" + url);
            return Jsoup.parse(new URL(url), 10000);

        } catch (Exception e) {
            System.out.println(e);
            return null;
        } finally {
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
            lock.unlock();
        }
    }

}
