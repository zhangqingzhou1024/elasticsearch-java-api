package com.search.es.query.common;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 关键词规则转化
 * 作用：
 *
 * @author zqz
 */
public class KeywordsFormat {

    /**
     * 每组关键词只能包括一个乘号(*)和减号(-)
     *
     * @param keywords
     * @return 返回值大于0表示某组关键词内不符合“每组关键词只能包括一个乘号(*)和减号(-)”的要求
     */
    public int validateKeywordsFormat(String keywords) {
        int result = -1;

        if (StringUtils.isNotBlank(keywords)) {
            String[] keywordsArr = keywords.replaceAll("，", ",")
                    .replaceAll("；", ";").split(";");
            for (int i = 0; i < keywordsArr.length; i++) {
                String keyword = keywordsArr[i].replaceAll("（", "(")
                        .replaceAll("）", ")").replaceAll("\\)\\s*\\(", ")(");
                if (StringUtils.countMatches(keyword, "*") > 1
                        || StringUtils.countMatches(keyword, "-") > 1) {
                    return 1;
                }
                if (keyword.indexOf(")(") > -1) {
                    return 2;
                }
            }
        }
        return result;
    }


    /**
     * 关键词转换
     */
    public List<String[]> exchangeKws(String data) {

        List<String[]> list = new ArrayList<String[]>();
        if (data != null) {
            //String exp = "(a & b|c&d)|(b|d) &(c)&（x|y）&(z|w)|(d&v)&(a&c|d) !(m&d|c)";
            try {

                int validate = validateKeywordsFormat(data);
                if (validate > 0) {
                    return null;
                }
                String exp = data.replaceAll("，", ",");
                ExpressionConvert ec = new ExpressionConvert();
                ec.convert(exp);
                //首先对非关键词进行处理，所有的关键词都要去掉该非关键词
                List<String> list_no = ec.getNotKeywordsList();
                String nokws = "";
                if (list_no != null && list_no.size() > 0) {
                    for (String s_no : list_no) {
                        nokws += "|" + s_no.trim();
                        //nokws += "OR"+s_no.trim();
                    }
                }
                if (nokws.indexOf("|") != -1) {
                    nokws = nokws.substring(1);
                }
                //对包含的关键词进行处理，并对数据进行组装
                List<String> list_have = ec.getKeywordsList();
                if (list_have != null && list_have.size() > 0) {
                    for (String s_have : list_have) {
                        list.add(new String[]{"" + s_have.trim() + "", "" + nokws + ""});
                    }
                }
            } catch (Exception e) {
                list = null;
                e.printStackTrace();
            }
        }
        if (list != null) {
            for (Object[] objects : list) {

                System.out.println("包含：" + objects[0] + " 不包含：" + objects[1]);
            }
        }
        return list;
    }


    public static void main(String[] args) {
        KeywordsFormat keywordsFormat = new KeywordsFormat();
        String data = "(南宁|桂林)&(反腐&贪污|打劫)!(楼盘|广告)";
        data = "免疫球蛋白&(艾滋病|HIV)";
        data = "(南宁|桂林)&(反腐&贪污|打劫)!(楼盘|广告)";
        List<String[]> exchangeKws = keywordsFormat.exchangeKws(data);

        if (exchangeKws != null) {
            for (Object[] objects : exchangeKws) {

                System.out.println("包含：" + objects[0] + " 不包含：" + objects[1]);
            }
        }
    }
}
