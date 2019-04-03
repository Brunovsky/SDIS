package dbs;

public class Utils {

    private static final int leadingSpaces = 25;

    public static void printErr(String errorClass, String errorDescription) {
        System.err.print(String.format("%-" + leadingSpaces + "s", errorClass + ":"));
        System.err.println(errorDescription);
    }

    public static void printInfo(String infoClass, String infoDescription) {
        System.out.print(String.format("%-" + leadingSpaces + "s", infoClass + ":"));
        System.out.println(infoDescription);
    }
}