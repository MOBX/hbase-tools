/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.pojo;

import java.util.*;

/**
 * @author zxc Nov 12, 2015 11:50:17 AM
 */
public class PageHBase<T> {

    private int                  currentPageNo   = 1;                             // 当前页码
    private int                  pageSize        = 1000;                          // 每页显示行数
    private int                  totalCount      = 0;                             // 总行数
    private int                  totalPage       = 0;                             // 总页数
    private boolean              hasNext         = false;                         // 是否有下一页
    private String               nextPageRowkey;                                  // 下一页起始rowkey
    private List<T>              resultList;                                      // 结果集List
    // 每页对应的startRow，key为currentPageNo，value为Rowkey
    private Map<Integer, String> pageStartRowMap = new HashMap<Integer, String>();

    public PageHBase() {

    }

    public PageHBase(int currentPageNo, int pageSize) {
        setCurrentPageNo(currentPageNo);
        setPageSize(pageSize);
    }

    public int getCurrentPageNo() {
        return currentPageNo;
    }

    public void setCurrentPageNo(int currentPageNo) {
        this.currentPageNo = currentPageNo;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public int getTotalPage() {
        return totalPage;
    }

    public void setTotalPage(int totalPage) {
        this.totalPage = totalPage;
    }

    public boolean isHasNext() {
        return hasNext;
    }

    public void setHasNext(boolean hasNext) {
        this.hasNext = hasNext;
    }

    public String getNextPageRowkey() {
        return nextPageRowkey;
    }

    public void setNextPageRowkey(String nextPageRowkey) {
        this.nextPageRowkey = nextPageRowkey;
    }

    public List<T> getResultList() {
        return resultList;
    }

    public void setResultList(List<T> resultList) {
        this.resultList = resultList;
    }

    public Map<Integer, String> getPageStartRowMap() {
        return pageStartRowMap;
    }

    public void setPageStartRowMap(Map<Integer, String> pageStartRowMap) {
        this.pageStartRowMap = pageStartRowMap;
    }

    public void execute() {
        int n = totalCount / pageSize;
        if (totalCount % pageSize == 0) {
            totalPage = n;
        } else {
            totalPage = ((int) n) + 1;
        }
    }
}
