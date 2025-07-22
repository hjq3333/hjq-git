import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class mock {

    private static final String OUTPUT_FILE = "D:/Desktop/real_time_data.csv";
    private static final Random random = new Random();
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws InterruptedException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE, true))) {
            // 表头
            writer.write("id,timestamp,user_id,product_id,amount\n");

            // 持续生成数据
            for (int i = 0; i < 1000; i++) {
                long id = System.currentTimeMillis() * 1000 + random.nextInt(1000);
                String timestamp = dateFormat.format(new Date());
                int userId = random.nextInt(10000);
                int productId = random.nextInt(100);
                double amount = random.nextDouble() * 1000;

                writer.write(String.format("%d,%s,%d,%d,%.2f\n", id, timestamp, userId, productId, amount));
                writer.flush();

                System.out.println("Generated data: " + id);
                Thread.sleep(1000); // 每秒生成一条数据
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
