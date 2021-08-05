public class test {
    public static void main(String[] args) {
        Integer total = 120;

        Integer a = 77;

        double aRatio = Math.round(a * 1000D / total) / 10D;

//        double bRatio = Math.round((100 - aRatio) * 10D) / 10D;

//        System.out.println((a*1D / total));
        System.out.println(100D - aRatio);
        System.out.println("a:"+aRatio);
//        System.out.println("b:"+bRatio);
    }
}
