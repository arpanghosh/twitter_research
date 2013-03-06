package com.edge.twitter_research.collector_categories;

public class UserCategoryMessage implements Comparable<UserCategoryMessage> {

    long user_id;
    String category_slug;
    long since_id;

    public UserCategoryMessage(long user_id,
                               String category_slug,
                               long since_id){
        this.user_id = user_id;
        this.category_slug = category_slug;
        this.since_id = since_id;
    }


    public int compareTo(UserCategoryMessage userCategoryMessage){
        if (this.since_id < userCategoryMessage.since_id)
            return -1;
        else if (this.since_id > userCategoryMessage.since_id)
            return 1;
        else
            return 0;
    }


    @Override public boolean equals(Object object){
        if (object != null &&
                object instanceof UserCategoryMessage){
            UserCategoryMessage userCategoryMessage =
                    (UserCategoryMessage)object;
            return ((this.since_id == userCategoryMessage.since_id) &&
                    (this.user_id == userCategoryMessage.user_id) &&
                    (this.category_slug.equals(userCategoryMessage.category_slug)));
        }else
            return false;
    }
}
