package com.edge.twitter_research.event_detection;


import java.util.regex.Matcher;
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
        DELTA("delta", Area.AIR_TRAVEL, "\\bdelta\\b|\\bdelta's\\b"),
        AMERICAN_AIRLINES("american_airlines", Area.AIR_TRAVEL, "american airlines"),
        VIRGIN_AMERICA("virgin_america", Area.AIR_TRAVEL, "virgin america"),


        FORD("ford", Area.AUTOMOTIVE, "\\bford\\b|\\bford's\\b"),
        HONDA("honda", Area.AUTOMOTIVE, "honda"),
        TOYOTA("toyota", Area.AUTOMOTIVE, "toyota"),
        DODGE("dodge", Area.AUTOMOTIVE, "\\bdodge\\b|\\bdodge's\\b"),
        CHEVROLET("chevrolet", Area.AUTOMOTIVE, "chevrolet"),


        GOOGLE("google", Area.COMPUTERS_AND_ELECTRONICS, "google"),
        HP("hp", Area.COMPUTERS_AND_ELECTRONICS, "\\bhp\\b|\\bhp's\\b"),
        APPLE("apple", Area.COMPUTERS_AND_ELECTRONICS, "\\bapple\\b|\\bapple's\\b"),
        MICROSOFT("microsoft", Area.COMPUTERS_AND_ELECTRONICS, "microsoft"),
        DELL("dell", Area.COMPUTERS_AND_ELECTRONICS, "\\bdell\\b|\\bdell's\\b"),


        BANK_OF_AMERICA("bank_of_america", Area.PERSONAL_FINANCE, "\\Qbank of america\\E|\\bboa\\b"),
        WELLS_FARGO("wells_fargo", Area.PERSONAL_FINANCE, "wells fargo"),
        CHASE("chase", Area.PERSONAL_FINANCE, "\\bchase\\b|\\bchase's\\b"),
        CAPITAL_ONE("capital_one", Area.PERSONAL_FINANCE, "capital one"),
        AMERICAN_EXPRESS("american_express", Area.PERSONAL_FINANCE, "\\Qamerican express\\E|\\Qamex\\E"),


        GE("ge", Area.DURABLE_GOODS, "\\bge\\b|\\bge's\\b"),
        SEARS("sears", Area.DURABLE_GOODS, "sears"),
        KEURIG("keurig", Area.DURABLE_GOODS, "keurig"),
        ELECTROLUX("electrolux", Area.DURABLE_GOODS, "electrolux"),
        FRIGIDAIRE("frigidaire", Area.DURABLE_GOODS, "frigidaire"),


        SCHWAB("schwab", Area.FINANCIAL_PLANNING, "\\Qschwab\\E|\\Qcharles schwab\\E"),
        SMITH_BARNEY("smith_barney", Area.FINANCIAL_PLANNING, "smith barney"),
        FIDELITY("fidelity", Area.FINANCIAL_PLANNING, "fidelity"),
        JOHN_HANCOCK("john_hancock", Area.FINANCIAL_PLANNING, "john hancock"),
        AXA("axa", Area.FINANCIAL_PLANNING, "\\baxa\\b|\\baxa's\\b"),


        BLUE_CROSS_BLUE_SHIELD("blue_cross_blue_shield", Area.INSURANCE, "\\Qblue cross\\E|\\Qblue shield\\E|\\bbcbs\\b"),
        STATE_FARM("state_farm", Area.INSURANCE, "state farm"),
        GEICO("geico", Area.INSURANCE, "geico"),
        ANTHEM("anthem", Area.INSURANCE, "\\banthem\\b|\\banthem's\\b"),
        HUMANA("humana", Area.INSURANCE, "humana"),


        CARTIER("cartier", Area.LUXURY_GOODS, "cartier"),
        MONT_BLANC("mont_blanc", Area.LUXURY_GOODS, "mont blanc"),
        ROLEX("rolex", Area.LUXURY_GOODS, "rolex"),
        HUBLOT("hublot", Area.LUXURY_GOODS, "hublot"),
        LOUIS_VUITTON("louis_vuitton", Area.LUXURY_GOODS, "louis vuitton"),


        VERIZON("verizon", Area.MOBILE_AND_WIRELESS, "verizon"),
        BLACKBERRY("blackberry", Area.MOBILE_AND_WIRELESS, "blackberry"),
        SPRINT("sprint", Area.MOBILE_AND_WIRELESS, "\\bsprint\\b|\\bsprint's\\b"),
        SAMSUNG("samsung", Area.MOBILE_AND_WIRELESS, "samsung"),
        IPHONE("iphone", Area.MOBILE_AND_WIRELESS, "iphone"),


        EBAY("ebay", Area.SHOPPING, "\\bebay\\b|\\bebay's\\b"),
        AMAZON("amazon", Area.SHOPPING, "amazon"),
        WALMART("walmart", Area.SHOPPING, "walmart"),
        TARGET("target", Area.SHOPPING, "\\btarget\\b|\\btarget's\\b"),
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


        YELP("yelp", Area.FOOD_AND_DRINK, "\\byelp\\b|\\byelp's\\b"),
        DOMINOS("dominos", Area.FOOD_AND_DRINK, "dominos"),
        PAPA_JOHNS("papa_johns", Area.FOOD_AND_DRINK, "papa john"),
        PIZZA_HUT("pizza_hut", Area.FOOD_AND_DRINK, "pizza hut"),
        MCDONALDS("mcdonalds", Area.FOOD_AND_DRINK, "\\Qmc donalds\\E|\\Qmcdonalds\\E"),


        HOME_DEPOT("home_depot", Area.HOME_AND_GARDEN, "home depot"),
        LOWES("lowes", Area.HOME_AND_GARDEN, "\\blowes\\b|\\blowe's\\b"),
        IKEA("ikea", Area.HOME_AND_GARDEN, "\\bikea\\b|\\bikea's\\b"),
        BED_BATH_AND_BEYOND("bed_bath_beyond", Area.HOME_AND_GARDEN, "\\Qbed bath and beyond\\E|\\Qbed bath & beyond\\E"),
        ASHLEY_FURNITURE("ashley_furniture", Area.HOME_AND_GARDEN, "ashley furniture");


        public final String name;
        public final Matcher patternMatcher;
        public final Area area;

        Company(String company, Area area, String pattern){
            this.name = company;
            this.area = area;
            this.patternMatcher = Pattern.compile(pattern).matcher("");
        }
    }



}
