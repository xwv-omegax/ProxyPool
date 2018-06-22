package xwv.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.TextNode;
import xwv.proxy.Proxy;
import xwv.proxy.service.ProxyPoolService;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class _66daili extends SimpleCrawler {

    private static final Lock lock = new ReentrantLock(true);


    public _66daili() {
    }

    @Override
    public ConcurrentLinkedQueue<String> nextUrlQueue() {
        return new ConcurrentLinkedQueue<String>() {{
            offer("http://www.66ip.cn/mo.php?tqsl=300");
        }};
    }

    @Override
    public boolean hasNextUrlQueue() {
        return true;
    }

    @Override
    public void reset() {
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
            List<TextNode> nodes = document.selectFirst("body").textNodes();
            for (TextNode node : nodes) {
                String[] arr = node.text().trim().split(":");
                if (arr.length > 1) {
                    Proxy proxyInfo = new Proxy(arr[0], Integer.parseInt(arr[1]));
                    getExecutor().execute(new Runnable() {
                        @Override
                        public void run() {
                            service.put(proxyInfo.toString());
                        }
                    });
                }
            }

//            Elements pagination = document.selectFirst(".pagination").children();
//            int size = pagination.size();
//            int maxPage = Integer.parseInt(pagination.get(size - 2)
//                    .text());
//            if (page >= maxPage) {
//                page = 0;
//            }
//            Elements elements = document.select("#ip_list").select("tbody tr");
//            for (Element e : elements) {
//                if (e.child(1).tagName().equals("th")) {
//                    continue;
//                }
//                String host = e.child(1).text();
//                int port = Integer.parseInt(e.child(2).text());
//                if (service.testProxy(host, port)) {
//                    Proxy proxyInfo = new Proxy(host, port);
//                    service.put(proxyInfo);
//                }
//            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Document parse(String url) throws IOException {
//        System.out.println(Thread.currentThread().getName());
        lock.lock();
        try {
//            System.out.println("parse:" + url);
            return Jsoup.parse(new URL(url), 1000);

        } catch (Exception e) {
            System.out.println(e);
            return null;
        } finally {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
            lock.unlock();
        }
    }

    public static void main(String[] args) {
        _66daili c = new _66daili();
        while (c.hasNextUrlQueue()) {
            for (String u : c.nextUrlQueue()) {
                c.parseOne(u);
            }
        }
    }

}
