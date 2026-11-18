package com.pinchez.wallet.loadgen.service;

import com.pinchez.wallet.merchant.enums.MerchantCategory;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.random.RandomGenerator;

final class FakeData {

    private static final List<String> FIRST_NAMES = List.of(
            "Wanjiku",
            "Kamau",
            "Otieno",
            "Akinyi",
            "Mutua",
            "Njeri",
            "Kiprop",
            "Chebet",
            "Wekesa",
            "Nafula",
            "Omondi",
            "Atieno",
            "Mwangi",
            "Wambui",
            "Kibet",
            "Jepkosgei",
            "Barasa",
            "Muthoni",
            "Odhiambo",
            "Achieng",
            "Halima",
            "Abdi",
            "Fatuma",
            "Juma",
            "Grace",
            "Brian",
            "Faith",
            "Kevin",
            "Mercy",
            "Dennis");

    private static final List<String> LAST_NAMES = List.of(
            "Kamau",
            "Ochieng",
            "Mutua",
            "Wanjala",
            "Kiptoo",
            "Njoroge",
            "Otieno",
            "Mwangi",
            "Cheruiyot",
            "Onyango",
            "Kariuki",
            "Wafula",
            "Maina",
            "Kiprono",
            "Achieng",
            "Hassan",
            "Mohamed",
            "Omondi",
            "Kimani",
            "Nyaga");

    private static final List<MerchantCategory> CATEGORIES = List.of(MerchantCategory.values());

    private FakeData() {}

    static String phoneNumber(RandomGenerator random) {
        var prefix = random.nextInt(10) < 8 ? "2547" : "2541";
        return prefix + "%08d".formatted(random.nextInt(100_000_000));
    }

    static String fullName(RandomGenerator random) {
        return pick(FIRST_NAMES, random) + " " + pick(LAST_NAMES, random);
    }

    static String tillNumber(RandomGenerator random) {
        return String.valueOf(random.nextInt(100_000, 1_000_000));
    }

    static String agentNumber(RandomGenerator random) {
        return String.valueOf(random.nextInt(100_000, 1_000_000));
    }

    static MerchantCategory merchantCategory(RandomGenerator random) {
        return pick(CATEGORIES, random);
    }

    static String merchantName(MerchantCategory category, RandomGenerator random) {
        var templates = switch (category) {
            case GROCERY -> List.of("%s Groceries", "%s Mini Mart", "Mama %s Fresh Produce");
            case BETTING -> List.of("%s Bets", "Lucky %s Gaming");
            case FUEL -> List.of("%s Petrol Station", "%s Fuel Point");
            case RESTAURANT -> List.of("%s Kibanda", "%s Nyama Choma Joint", "%s Cafe");
            case TRANSPORT -> List.of("%s Sacco", "%s Boda Riders");
            case UTILITIES -> List.of("%s Water Services", "%s Power Tokens");
            case PHARMACY -> List.of("%s Chemist", "%s Pharmacy");
        };
        return pick(templates, random).formatted(pick(LAST_NAMES, random));
    }

    static BigDecimal amount(RandomGenerator random, double median, double spread) {
        var raw = median * Math.exp(spread * random.nextGaussian());
        var clamped = Math.clamp(Math.round(raw), 10L, 150_000L);
        return BigDecimal.valueOf(clamped).setScale(2, RoundingMode.UNNECESSARY);
    }

    static <T> T pick(List<T> values, RandomGenerator random) {
        return values.get(random.nextInt(values.size()));
    }
}
