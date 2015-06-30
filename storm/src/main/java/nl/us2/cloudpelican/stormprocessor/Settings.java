package nl.us2.cloudpelican.stormprocessor;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Created by robin on 30/06/15.
 */
public class Settings implements Serializable {
    private static final long serialVersionUID = 7526472295622776147L;

    private static JsonObject data;
    private String _json;

    public void load(JsonObject data) {
        this.data = data;
    }

    public String get(String k) {
        if (data.has(k) && data.get(k).isJsonPrimitive()) {
            return data.get(k).getAsString();
        }
        return null;
    }

    public String getOrDefault(String k, String d) {
        if (data.has(k) && data.get(k).isJsonPrimitive()) {
            return data.get(k).getAsString();
        }
        return d;
    }

    /**
     * Always treat de-serialization as a full-blown constructor, by
     * validating the final state of the de-serialized object.
     */
    private void readObject(
            ObjectInputStream aInputStream
    ) throws ClassNotFoundException, IOException {
        //always perform the default de-serialization first
        aInputStream.defaultReadObject();

        // restore object
        JsonParser jp = new JsonParser();
        this.data = jp.parse(_json).getAsJsonObject();
    }

    private void writeObject(
            ObjectOutputStream aOutputStream
    ) throws IOException {
        //perform the default serialization for all non-transient, non-static fields
        _json = this.data.toString();
        aOutputStream.defaultWriteObject();
    }
}
