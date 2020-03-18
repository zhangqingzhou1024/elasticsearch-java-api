package com.search.es.query.common;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zqz
 * @version 1.0
 * @date 2020-03-12 16:51
 */
@Data
public class ExpressionConvert {
    private List<String> keywordsList = new ArrayList();
    private List<String> notKeywordsList = new ArrayList();


    public void convert(String kws) throws Exception {
        String keywords = kws.replace("（", "(").replace("）", ")").replace("！", "!").replace("＆", "&").replace("｜", "|").replace(" ", "");
        if (keywords.contains("!")) {
            String keywordNot = keywords.substring(keywords.indexOf("!") + 1, keywords.length());
            keywordNot = keywordNot.replaceAll("\\!", "");
            keywords = keywords.substring(0, keywords.indexOf("!"));
            this.notKeywordsList = this.executeSplit(keywordNot);
        }

        this.keywordsList = this.executeSplit(keywords);
    }

    public List<String> executeSplit(String keyword) throws Exception {
        List<String> res = new ArrayList();
        if (!Helper.hasBracket(keyword)) {
            res = this.baseSplit(keyword);
        } else {
            List<String> fList = this.groupSplit(keyword, "|");
            Iterator var5 = fList.iterator();

            while (var5.hasNext()) {
                String f = (String) var5.next();
                new ArrayList();
                List<List<String>> temp = new ArrayList();
                List<String> sList = this.groupSplit(f, "&");
                Iterator var9 = sList.iterator();

                while (var9.hasNext()) {
                    String s = (String) var9.next();
                    new ArrayList();
                    if (Helper.hasBracket(s)) {
                        s = Helper.removeAllBracket(s);
                    }

                    List<String> bLists = this.baseSplit(s);
                    temp.add(bLists);
                }

                ((List) res).addAll(this.descartes(temp));
            }
        }

        return (List) res;
    }

    public List<String> baseSplit(String keyword) throws Exception {
        List<String> klist = new ArrayList();
        String[] ksh = new String[0];
        if (keyword.contains("|")) {
            ksh = keyword.split("\\|");
            String[] var7 = ksh;
            int var6 = ksh.length;

            for (int var5 = 0; var5 < var6; ++var5) {
                String ss = var7[var5];
                if (ss.contains("&")) {
                    ss = ss.replace("&", " ");
                }

                klist.add(ss.trim());
            }
        } else if (keyword.contains("&")) {
            keyword = keyword.replace("&", " ");
            klist.add(keyword.trim());
        } else {
            klist.add(keyword.trim());
        }

        return klist;
    }


    public List<String> groupSplit(String keyword, String splitFlag) throws Exception {
        List<String> res = new ArrayList();
        String[] keywords = keyword.split("\\" + splitFlag);
        List<KeyWords> kwords = new ArrayList();

        int i;
        int j;
        for (i = 0; i < keywords.length; ++i) {
            KeyWords k = new KeyWords();
            j = Helper.getSubNumber(keywords[i], "\\(") + Helper.getSubNumber(keywords[i], "\\)");
            k.setKeyword(keywords[i]);
            k.setSumBlacet(j);
            kwords.add(k);
        }

        for (i = 0; i < kwords.size(); ++i) {
            if (((KeyWords) kwords.get(i)).getSumBlacet() % 2 == 0) {
                res.add(((KeyWords) kwords.get(i)).getKeyword());
            } else {
                String s = ((KeyWords) kwords.get(i)).getKeyword();

                for (j = i + 1; j < kwords.size(); ++j) {
                    s = s + splitFlag + ((KeyWords) kwords.get(j)).getKeyword();
                    if (((KeyWords) kwords.get(j)).getSumBlacet() % 2 != 0) {
                        i = j;
                        res.add(s);
                        break;
                    }
                }
            }
        }

        return res;
    }

    public List<String> descartes(List<List<String>> lists) throws Exception {
        if (lists != null && lists.size() >= 1) {
            List<String> l1 = null;
            Iterator var4 = lists.iterator();

            while (var4.hasNext()) {
                List<String> list = (List) var4.next();
                if (l1 == null) {
                    l1 = list;
                } else {
                    l1 = this.mutiply(l1, list);
                }
            }

            return l1;
        } else {
            return null;
        }
    }

    public List<String> mutiply(List<String> l1, List<String> l2) throws Exception {
        if (l1 != null && l2 != null && l1.size() >= 1 && l2.size() >= 1) {
            List<String> ret = new ArrayList();
            Iterator var5 = l1.iterator();

            while (var5.hasNext()) {
                String str1 = (String) var5.next();
                Iterator var7 = l2.iterator();

                while (var7.hasNext()) {
                    String str2 = (String) var7.next();
                    ret.add(str1 + " " + str2);
                }
            }

            return ret;
        } else {
            return null;
        }
    }
}


class Helper {
    static boolean  hasBracket(String keyWord) {
        boolean res = false;
        if (keyWord.contains("(") | keyWord.contains(")")) {
            res = true;
        }
        return res;
    }

    static String removeAllBracket(String keyWord) {
        if (keyWord.contains(")")) {
            keyWord = keyWord.replaceAll("\\)", "");
        }
        if (keyWord.contains("(")) {
            keyWord = keyWord.replaceAll("\\(", "");
        }
        return keyWord;
    }

    static int getSubNumber(String des, String reg) {
        Pattern p = Pattern.compile(reg);
        Matcher m = p.matcher(des);

        int count;
        for (count = 0; m.find(); ++count) {
        }

        return count;
    }
}

@Data
@NoArgsConstructor
class KeyWords {
    private String keyword;
    private int sumBlacet;
    private int flag;
}

