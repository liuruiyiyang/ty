package Kyro;

import storm.IoTDBTransferAndStoreBolt;
import ty.pub.TransPacket;

import java.util.Date;
import java.util.HashMap;

public class IoTDBTransPacketTest {
    public static void main(String[] args) {
        TransPacket packet = new TransPacket();
        packet.setDeviceId("110");
        packet.setTimestamp(new Date().getTime());
        packet.setIp("192.168.15.199");
        packet.setTimestamp(14235145);
        packet.setDeviceId("1");
        packet.addBaseInfo("1", "1");
        packet.addBaseInfo("2", "2");
        packet.addWorkStatus("1", 10, "1");
        packet.addWorkStatus("1", 15, "11");
        packet.addWorkStatus("3", 10, "3");
        packet.addWorkStatus("4", 1, "4");
        packet.addWorkStatus("4", 10, "44");
        HashMap<String, String> baseInfoMap = new HashMap<>();
        baseInfoMap.put("1","1345145");
        packet.setBaseInfoMap(baseInfoMap);
        IoTDBTransferAndStoreBolt ioTDBTransferAndStoreBolt = new IoTDBTransferAndStoreBolt();

        ioTDBTransferAndStoreBolt.IoTDBInsert(packet);

    }
}
