package org.pross.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @describe:
 * @author:彭爽pross
 * @date: 2018/11/26
 */
public class TestYARNClient {
	private static Logger logger = LoggerFactory.getLogger(TestYARNClient.class);

//	private static final String RM1_NODE_ID = "172.18.84.138";
//	private static final int RM1_PORT_BASE = 10000;
//	private static final String RM2_NODE_ID = "172.18.84.139";
//	private static final int RM2_PORT_BASE = 20000;

	private static Configuration conf;
	private static YarnClient client;

	/**
	 * 初始化连接和设置
	 */
	public void setup() {
		conf = new YarnConfiguration();
		Configuration conf = new YarnConfiguration(this.conf);
		client = YarnClient.createYarnClient();
		client.init(conf);
		client.start();
	}

	// get allQueues
	public void getAllQueues() {
		logger.info("[yarn getAllQueues %s]");
		try {
			//得到所有的队列信息
			List<QueueInfo> allQueues = client.getAllQueues();
			for (QueueInfo queues : allQueues) {
				System.out.println(queues.getQueueName()+";"+queues.getApplications());
			}
			return;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//get applications
	public void getApplications(){
		logger.info("[yarn getApplication  %s]");
		try {
			List<ApplicationReport> applications = client.getApplications();
			for(ApplicationReport app:applications){
				System.out.println(app.getApplicationId()+";"+app.getName());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (YarnException e) {
			e.printStackTrace();
		}
	}

	// kill application
	public void killApplication(String applicationId) throws Exception {
		logger.info("[yarn application -kill %s]", applicationId);
		try {
			client.killApplication(ConverterUtils.toApplicationId(applicationId));
		} catch (YarnException e) {
			e.printStackTrace();
		}
	}


	// get ClusterNodeLabels
	public void getClusterNodeLabels(){
		logger.info("[yarn getAMEMToken %s]");
		try {
			Set<String> clusterNodeLabels = client.getClusterNodeLabels();
			Iterator<String> iterator = clusterNodeLabels.iterator();
			while(iterator.hasNext()){
				String next = iterator.next();
				System.out.println(next);
			}
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//test kill application of runningtask and username not owner
	public void killApplicationOthers(){
		EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
		if(appStates.isEmpty()){
			appStates.add(YarnApplicationState.RUNNING);
		}
		try {
			List<ApplicationReport> applications = client.getApplications(appStates);
			for(ApplicationReport app:applications){
				if(app.getName().equalsIgnoreCase("jobName")){
					ApplicationId applicationId = app.getApplicationId();
					client.killApplication(applicationId);
				}else{
					continue;
				}
			}
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// get running applications
	public void getRunningApplication(){
		EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
		if(appStates.isEmpty()){
			// 添加状态，运行中
			appStates.add(YarnApplicationState.RUNNING);
			// 失败任务
			appStates.add(YarnApplicationState.FAILED);
		}
		try {
			List<ApplicationReport> applications = client.getApplications(appStates);
			for(ApplicationReport app:applications){
				System.out.println(app.getName());
			}
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) throws Exception {
		//set local env
		System.setProperty("hadoop.home.dir", "/Users/pengshuang/Documents/env/hadoop265");
		TestYARNClient yarnClient = new TestYARNClient();
		yarnClient.setup();
//		yarnClient.getAllQueues();
		yarnClient.getApplications();
//		yarnClient.getRunningApplication();
//		yarnClient.killApplicationOthers();
		client.stop();
	}

}
