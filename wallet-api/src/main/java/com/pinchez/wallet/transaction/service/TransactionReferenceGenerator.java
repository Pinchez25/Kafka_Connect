package com.pinchez.wallet.transaction.service;

import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Component;

@Component
class TransactionReferenceGenerator {

    private static final String ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    private static final int LENGTH = 10;

    String next() {
        var random = ThreadLocalRandom.current();
        var reference = new StringBuilder(LENGTH);
        for (int i = 0; i < LENGTH; i++) {
            reference.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
        }
        return reference.toString();
    }
}
