import com.rabbitmq.client.Delivery;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class TaskSaver {
    private final Map<String, Consumer<Delivery>> taskMap;

    public TaskSaver() {
        this.taskMap = new HashMap<>();
    }

    public void saveTask(String correlationId, Consumer<Delivery> task) {
        taskMap.put(correlationId, task);
    }

    public void executeTask(String correlationId, Delivery delivery) {
        Consumer<Delivery> task = taskMap.get(correlationId);
        if (task != null) {
            task.accept(delivery);
            taskMap.remove(correlationId);
        }
    }
}
