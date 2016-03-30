package example.pig;

import java.util.Map;

/**
 * Created by jackeylyu on 2016/3/10.
 */
public class UrlParamDecoder {

    public static void decodeSplit(String content, Map<String, String> map){
        String[] kvs = content.split("&");
        for (String kv : kvs){
            String[] pair = kv.split("=");
            map.put(pair[0], pair[1]);
        }
    }

    public static void decodeTo(String content, Map<String, String> map) {

        synchronized (map) {
            String key = null;
            String value = null;
            int mark = -1;
            // boolean encoded = false;
            for (int i = 0; i < content.length(); i++) {
                char c = content.charAt(i);
                switch (c) {
                    case '&':
                        int l = i - mark - 1;
                        // value = l == 0 ? "" : (encoded ? decodeString(content,
                        // mark + 1, l, charset) : content.substring(mark + 1,
                        // i));
                        value = l == 0 ? "" : content.substring(mark + 1, i);
                        mark = i;
                        // encoded = false;
                        if (key != null) {
                            map.put(key, value);
                        } else if (value != null && value.length() > 0) {
                            // 应对这种情况a&b=2
                            map.put(value, "");
                        }
                        key = null;
                        value = null;
                        break;
                    case '=':
                        if (key != null)
                            break;
                        // key = encoded ? decodeString(content, mark + 1, i - mark
                        // - 1, charset) : content.substring(mark + 1, i);
                        key = content.substring(mark + 1, i);
                        mark = i;
                        // encoded = false;
                        break;
                    // case '+':
                    // encoded = true;
                    // break;
                    // case '%':
                    // encoded = true;
                    // break;
                }
            }

            if (key != null) {
                // 处理最后一个=后面的内容
                int l = content.length() - mark - 1;
                // value = l == 0 ? "" : (encoded ? decodeString(content,
                // mark + 1, l, charset) : content.substring(mark + 1));
                value = l == 0 ? "" : content.substring(mark + 1);
                map.put(key, value);
            } else if (mark < content.length()) {
                // 如果最后一个符号是&， 则把&之后的一段内容作为key
                // key = encoded ? decodeString(content, mark + 1,
                // content.length() - mark - 1, charset) : content
                // .substring(mark + 1);
                key = content.substring(mark + 1);
                if (key != null && key.length() > 0) {
                    map.put(key, "");
                }
            }
        }
    }
}
