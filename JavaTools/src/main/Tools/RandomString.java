package main.Tools;

import java.security.*;

public class RandomString {
    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static SecureRandom rnd = new SecureRandom();

    public static String randomString(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(AB.charAt(rnd.nextInt(AB.length())));
        return sb.toString();
    }

    public static String randomName() {
        int maxNameLen = 10;
        int maxNameLevel = 4;
        String name = "";
        for (int i = 0; i < rnd.nextInt(maxNameLevel) + 1; i++) {
            name += randomString(rnd.nextInt(maxNameLen)+1);
            name += ".";
        }
        return name;
    }

    public static void main(String args[]) {
        System.out.println(randomName());
    }
}
