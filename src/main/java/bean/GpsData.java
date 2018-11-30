package bean;

import java.io.Serializable;

public class GpsData implements Comparable<GpsData>, Serializable {
    private long deviceId;
    private long time;
    private String location;

    public long getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(long deviceId) {
        this.deviceId = deviceId;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }


    @Override
    public int compareTo(GpsData o) {
        int ans = (int) (deviceId - o.deviceId);
        if(ans!=0){
            return ans;
        }
        ans = (int) (time - o.time);
        if(ans!=0){
            return ans;
        }

        return location.compareTo(o.location);
    }
}
