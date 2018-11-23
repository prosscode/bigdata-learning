import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author pross shawn
 *
 * create time：2018年3月19日
 *
 * content：Version实体类
 */
public class Version implements WritableComparable<Version>{
	private String id;
	private String name;
	private String game;
	private int hour;
	private String source;
	private String version;
	private String city;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getGame() {
		return game;
	}
	public void setGame(String game) {
		this.game = game;
	}
	public int getHour() {
		return hour;
	}
	public void setHour(int hour) {
		this.hour = hour;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public Version(String id, String name, String game, int hour, String source, String version, String city) {
		super();
		this.id = id;
		this.name = name;
		this.game = game;
		this.hour = hour;
		this.source = source;
		this.version = version;
		this.city = city;
	}
	public Version() {
		super();
	}
	
	@Override
	public String toString() {
		return id + "," + name + "," + game + "," + hour + "," + source+ "," + version + "," + city;
	}
	
	/*
	 * 序列化与反序列化
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeUTF(name);
		out.writeUTF(game);
        out.writeInt(hour);  
        out.writeUTF(source);  
        out.writeUTF(version);  
        out.writeUTF(city);  
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readUTF();
		this.name = in.readUTF();
		this.game = in.readUTF();
		this.hour = in.readInt();
		this.source = in.readUTF();
		this.version = in.readUTF();
		this.city = in.readUTF();
	}
	
	/*
	 * 比较器 排序
	 */
	@Override
	public int compareTo(Version version) {
		int resultID=this.id.compareTo(version.getId());
		if(resultID==0){
			int resultName=this.name.compareTo(version.getName());
			if(resultName==0){
				return this.version.compareTo(version.getVersion());
			}else{
				return resultName;
			}
		}else{
			return resultID;
		}
	}
	
}
