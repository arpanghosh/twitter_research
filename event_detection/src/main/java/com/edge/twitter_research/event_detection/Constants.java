package com.edge.twitter_research.event_detection;


import java.util.regex.Pattern;

public class Constants {

    public static final String LOG4J_PROPERTIES_FILE_PATH =
            System.getProperty("user.home") + "/twitter_research/event_detection/log4j.properties";


    public static enum Area{
        AIR_TRAVEL("air_travel"),
        AUTOMOTIVE("automotive"),
        COMPUTERS_AND_ELECTRONICS("computers_electronics"),
        PERSONAL_FINANCE("personal_finance"),
        DURABLE_GOODS("durable_goods"),
        FINANCIAL_PLANNING("financial_planning"),
        INSURANCE("insurance"),
        LUXURY_GOODS("luxury_goods"),
        MOBILE_AND_WIRELESS("mobile_wireless"),
        SHOPPING("shopping"),
        ENTERTAINMENT("entertainment"),
        BUSINESS_AND_INDUSTRIAL("business_industrial"),
        FOOD_AND_DRINK("food_drink"),
        HOME_AND_GARDEN("home_garden");

        public final String name;
        Area(String area){this.name = area;}
    }


    public static enum Company{
        SOUTHWEST("southwest", Area.AIR_TRAVEL, "southwest"),
        UNITED_AIRLINES("united_airlines", Area.AIR_TRAVEL, "united airlines"),
        DELTA("delta", Area.AIR_TRAVEL, "delta"),
        AMERICAN_AIRLINES("american_airlines", Area.AIR_TRAVEL, "american airlines"),
        VIRGIN_AMERICA("virgin_america", Area.AIR_TRAVEL, "virgin america"),


        FORD("ford", Area.AUTOMOTIVE, "ford"),
        HONDA("honda", Area.AUTOMOTIVE, "honda"),
        TOYOTA("toyota", Area.AUTOMOTIVE, "toyota"),
        DODGE("dodge", Area.AUTOMOTIVE, "dodge"),
        CHEVROLET("chevrolet", Area.AUTOMOTIVE, "chevrolet"),


        GOOGLE("google", Area.COMPUTERS_AND_ELECTRONICS, "google"),
        //HP("hp", Area.COMPUTERS_AND_ELECTRONICS, "hp"),
        APPLE("apple", Area.COMPUTERS_AND_ELECTRONICS, "apple"),
        MICROSOFT("microsoft", Area.COMPUTERS_AND_ELECTRONICS, "microsoft"),
        DELL("dell", Area.COMPUTERS_AND_ELECTRONICS, "dell"),


        BANK_OF_AMERICA("bank_of_america", Area.PERSONAL_FINANCE, "(\\Qbank of america\\E|\\Qboa\\E)"),
        WELLS_FARGO("wells_fargo", Area.PERSONAL_FINANCE, "wells fargo"),
        CHASE("chase", Area.PERSONAL_FINANCE, "chase"),
        CAPITAL_ONE("capital_one", Area.PERSONAL_FINANCE, "capital one"),
        AMERICAN_EXPRESS("american_express", Area.PERSONAL_FINANCE, "(\\Qamerican express\\E|\\Qamex\\E)"),


        //GE("ge", Area.DURABLE_GOODS, "ge"),
        SEARS("sears", Area.DURABLE_GOODS, "sears"),
        KEURIG("keurig", Area.DURABLE_GOODS, "keurig"),
        ELECTROLUX("electrolux", Area.DURABLE_GOODS, "electrolux"),
        FRIGIDAIRE("frigidaire", Area.DURABLE_GOODS, "frigidaire"),


        SCHWAB("schwab", Area.FINANCIAL_PLANNING, "(\\Qschwab\\E|\\Qcharles schwab\\E)"),
        SMITH_BARNEY("smith_barney", Area.FINANCIAL_PLANNING, "smith barney"),
        FIDELITY("fidelity", Area.FINANCIAL_PLANNING, "fidelity"),
        JOHN_HANCOCK("john_hancock", Area.FINANCIAL_PLANNING, "john hancock"),
        AXA("axa", Area.FINANCIAL_PLANNING, "axa"),


        BLUE_CROSS_BLUE_SHIELD("blue_cross_blue_shield", Area.INSURANCE, "(\\Qblue cross\\E|\\Qblue shield\\E|\\Qbcbs\\E)"),
        STATE_FARM("state_farm", Area.INSURANCE, "state farm"),
        GEICO("geico", Area.INSURANCE, "geico"),
        ANTHEM("anthem", Area.INSURANCE, "anthem"),
        HUMANA("humana", Area.INSURANCE, "humana"),


        CARTIER("cartier", Area.LUXURY_GOODS, "cartier"),
        MONT_BLANC("mont_blanc", Area.LUXURY_GOODS, "mont blanc"),
        ROLEX("rolex", Area.LUXURY_GOODS, "rolex"),
        HUBLOT("hublot", Area.LUXURY_GOODS, "hublot"),
        LOUIS_VUITTON("louis_vuitton", Area.LUXURY_GOODS, "louis vuitton"),


        VERIZON("verizon", Area.MOBILE_AND_WIRELESS, "verizon"),
        BLACKBERRY("blackberry", Area.MOBILE_AND_WIRELESS, "blackberry"),
        SPRINT("sprint", Area.MOBILE_AND_WIRELESS, "sprint"),
        SAMSUNG("samsung", Area.MOBILE_AND_WIRELESS, "samsung"),
        IPHONE("iphone", Area.MOBILE_AND_WIRELESS, "iphone"),


        EBAY("ebay", Area.SHOPPING, "ebay"),
        AMAZON("amazon", Area.SHOPPING, "amazon"),
        WALMART("walmart", Area.SHOPPING, "walmart"),
        TARGET("target", Area.SHOPPING, "target"),
        BEST_BUY("best_buy", Area.SHOPPING, "best buy"),


        NETFLIX("netflix", Area.ENTERTAINMENT, "netflix"),
        FACEBOOK("facebook", Area.ENTERTAINMENT, "facebook"),
        HULU("hulu", Area.ENTERTAINMENT, "hulu"),
        SPOTIFY("spotify", Area.ENTERTAINMENT, "spotify"),


        USPS("usps", Area.BUSINESS_AND_INDUSTRIAL, "usps"),
        UPS("ups", Area.BUSINESS_AND_INDUSTRIAL, "ups"),
        FEDEX("fedex", Area.BUSINESS_AND_INDUSTRIAL, "fedex"),
        PAYPAL("paypal", Area.BUSINESS_AND_INDUSTRIAL, "paypal"),
        STAPLES("staples", Area.BUSINESS_AND_INDUSTRIAL, "staples"),


        YELP("yelp", Area.FOOD_AND_DRINK, "yelp"),
        DOMINOS("dominos", Area.FOOD_AND_DRINK, "dominos"),
        PAPA_JOHNS("papa_johns", Area.FOOD_AND_DRINK, "papa john"),
        PIZZA_HUT("pizza_hut", Area.FOOD_AND_DRINK, "pizza hut"),
        MCDONALDS("mcdonalds", Area.FOOD_AND_DRINK, "(\\Qmc donalds\\E|\\Qmcdonalds\\E)"),


        HOME_DEPOT("home_depot", Area.HOME_AND_GARDEN, "home depot"),
        LOWES("lowes", Area.HOME_AND_GARDEN, "lowe"),
        IKEA("ikea", Area.HOME_AND_GARDEN, "ikea"),
        BED_BATH_AND_BEYOND("bed_bath_beyond", Area.HOME_AND_GARDEN, "(\\Qbed bath and beyond\\E|\\Qbed bath & beyond\\E)"),
        ASHLEY_FURNITURE("ashley_furniture", Area.HOME_AND_GARDEN, "ashley furniture");


        public final String name;
        public final Pattern pattern;
        public final Area area;

        Company(String company, Area area, String pattern){
            this.name = company;
            this.area = area;
            this.pattern = Pattern.compile(pattern);
        }
    }


}
