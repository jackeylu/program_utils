
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class SystemUtils {
  
  private SystemUtils() {}
  
  public static long getPID() {
    String processName =
      java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
	System.out.println(processName);
    return Long.parseLong(processName.split("@")[0]);
  }

  public static void getId() {
		String sink_id = "";
		
		// get the IP
		try {
			InetAddress addr = InetAddress.getLocalHost();
			sink_id +=addr.getHostAddress();
		} catch (UnknownHostException e) {
			sink_id += "UnknownHost";
		}
		
		// get the pid		
		try {
			java.lang.management.RuntimeMXBean runtime =
					java.lang.management.ManagementFactory.getRuntimeMXBean();
			
			java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
			jvm.setAccessible(true);
			sun.management.VMManagement mgmt =  
					(sun.management.VMManagement) jvm.get(runtime);
			java.lang.reflect.Method pid_method =  
					mgmt.getClass().getDeclaredMethod("getProcessId");
			pid_method.setAccessible(true);

			sink_id += pid_method.invoke(mgmt);
			
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		
		// get the tid
		System.out.println("SinkID = " + sink_id);
	}
	
  public static void main(String[] args) {
    String msg = "My PID is " + SystemUtils.getPID();
    
	SystemUtils.getId();
	
    javax.swing.JOptionPane.showConfirmDialog((java.awt.Component)
        null, msg, "SystemUtils", javax.swing.JOptionPane.DEFAULT_OPTION);

  }

}