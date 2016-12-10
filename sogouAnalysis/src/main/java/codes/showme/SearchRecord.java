package codes.showme;

import java.io.Serializable;

/**
 * Created by jack on 12/10/16.
 */
public class SearchRecord implements Serializable {

    private static final long serialVersionUID = -9152786748871216711L;
    private String userId;
    private String time;
    private String queryWord;
    private String userClickUrl;
    private int rankOfUrl;
    private int clickOrder;

    public SearchRecord userId(String userId) {
        this.userId = userId;
        return this;
    }

    public SearchRecord time(String time) {
        this.time = time;
        return this;
    }

    public SearchRecord queryWord(String queryWord) {
        this.queryWord = queryWord;
        return this;
    }

    public SearchRecord userClickUrl(String userClickUrl) {
        this.userClickUrl = userClickUrl;
        return this;
    }

    public SearchRecord rankOfUrl(int rankOfUrl) {
        this.rankOfUrl = rankOfUrl;
        return this;
    }

    public SearchRecord clickOrder(int clickOrder) {
        this.clickOrder = clickOrder;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public String getTime() {
        return time;
    }

    public String getQueryWord() {
        return queryWord;
    }

    public String getUserClickUrl() {
        return userClickUrl;
    }

    public int getRankOfUrl() {
        return rankOfUrl;
    }

    public int getClickOrder() {
        return clickOrder;
    }


    @Override
    public String toString() {
        return "SearchRecord{" +
                "userId='" + userId + '\'' +
                ", time='" + time + '\'' +
                ", queryWord='" + queryWord + '\'' +
                ", userClickUrl='" + userClickUrl + '\'' +
                ", rankOfUrl=" + rankOfUrl +
                ", clickOrder=" + clickOrder +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SearchRecord that = (SearchRecord) o;

        if (rankOfUrl != that.rankOfUrl) return false;
        if (clickOrder != that.clickOrder) return false;
        if (userId != null ? !userId.equals(that.userId) : that.userId != null) return false;
        if (time != null ? !time.equals(that.time) : that.time != null) return false;
        if (queryWord != null ? !queryWord.equals(that.queryWord) : that.queryWord != null) return false;
        return !(userClickUrl != null ? !userClickUrl.equals(that.userClickUrl) : that.userClickUrl != null);

    }

    @Override
    public int hashCode() {
        int result = userId != null ? userId.hashCode() : 0;
        result = 31 * result + (time != null ? time.hashCode() : 0);
        result = 31 * result + (queryWord != null ? queryWord.hashCode() : 0);
        result = 31 * result + (userClickUrl != null ? userClickUrl.hashCode() : 0);
        result = 31 * result + rankOfUrl;
        result = 31 * result + clickOrder;
        return result;
    }
}
