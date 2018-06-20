package com.migu.schedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

/*
*类名和方法不能修改
 */
public class Schedule
{
    // 实例化
    
    private List<Integer> nodes = new ArrayList<Integer>();
    
    private List<Integer> tasks = new ArrayList<Integer>();
    
    private Map<Integer, List<TaskInfo>> taskStatus = new HashMap<Integer, List<TaskInfo>>();
    
    private Map<Integer, Integer> taskMap = new HashMap<Integer, Integer>();
    
    private Map<Integer, List<Integer>> sameTasks = new HashMap<Integer, List<Integer>>();
    
    private int threshold = 0;
    
    Comparator<TaskInfo> comparator = new Comparator<TaskInfo>()
    {
        public int compare(TaskInfo o1, TaskInfo o2)
        {
            return (o1.getTaskId() - o2.getTaskId());
        }
    };
    
    Comparator<TaskInfo> comparatorByNodeId = new Comparator<TaskInfo>()
    {
        public int compare(TaskInfo o1, TaskInfo o2)
        {
            return (o1.getNodeId() - o2.getNodeId());
        }
    };
    
    Comparator<Integer> comparatorByTime = new Comparator<Integer>()
    {
        public int compare(Integer o1, Integer o2)
        {
            return (taskMap.get(o2) - taskMap.get(o1));
        }
    };
    
    /**
     * 
     * 第一题，系统初始化
     *
     * @author wangzejun
     * @return
     */
    public int init()
    {
        // 初始化成功
        return ReturnCodeKeys.E001;
    }
    
    /**
     * 
     * 第二题，服务节点注册
     *
     * @author wangzejun
     * @param nodeId
     * @return
     */
    public int registerNode(int nodeId)
    {
        // 如果服务节点编号小于等于0
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        
        // 如果服务节点编号已注册
        if (nodes.contains(nodeId))
        {
            return ReturnCodeKeys.E005;
        }
        nodes.add(nodeId);
        // 排序
        Collections.sort(nodes);
        // 注册成功
        return ReturnCodeKeys.E003;
    }
    
    /**
     * 
     * 第三题，服务节点注销
     *
     * @author wangzejun
     * @param nodeId
     * @return
     */
    public int unregisterNode(int nodeId)
    {
        // 如果服务节点编号小于等于0
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        
        // 如果服务节点编号未被注册
        if (!nodes.contains(nodeId))
        {           
            return ReturnCodeKeys.E007;
        }
        // 移除节点
        nodes.remove(new Integer(nodeId));
        // 注销成功
        return ReturnCodeKeys.E006;
    }
    
    /**
     * 
     * 第四题，添加任务
     *
     * @author wangzejun
     * @param taskId
     * @param consumption
     * @return
     */
    public int addTask(int taskId, int consumption)
    {
        // 如果任务编号小于等于0
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;           
        }
        // 如果相同任务编号任务已经被添加
        if (tasks.contains(taskId))
        {           
            return ReturnCodeKeys.E010;
        }
        tasks.add(taskId);
        taskMap.put(taskId, consumption);
        Collections.sort(tasks, comparatorByTime);
        // 添加成功
        return ReturnCodeKeys.E008;
    }
    
    /**
     * 
     * 第五题，删除任务
     *
     * @author wangzejun
     * @param taskId
     * @return
     */
    public int deleteTask(int taskId)
    {
        // 如果任务编号小于等于0
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;           
        }
        
        // 如果指定编号的任务未被添加
        if (!tasks.contains(taskId))
        {
            return ReturnCodeKeys.E012;
        }
        tasks.remove(new Integer(taskId));
        taskMap.remove(new Integer(taskId));
        // 删除成功
        return ReturnCodeKeys.E011;
    }   
     
    /**
     * 
     * 第六题，任务调度
     *
     * @author wangzejun
     * @param threshold
     * @return
     */
    public int scheduleTask(int threshold)
    {
        if (tasks.isEmpty())
        {         
            return ReturnCodeKeys.E014;
        }
        this.threshold = threshold;
        boolean balanced = false;
        
        List<Integer> tmpTasks = new ArrayList<Integer>();
        for (Integer taskId : tasks)
        {
            tmpTasks.add(taskId);
        }
        for (Integer nodeId : nodes)
        {
            List<TaskInfo> taskInfos = new ArrayList<TaskInfo>();
            taskStatus.put(nodeId, taskInfos);
        }
        
        /*while (!balanced || tmpTasks.size() > 0)
        {
            for (Integer taskId : tmpTasks)
            {
                int nodeId = findNode();
                List<TaskInfo> taskInfos = taskStatus.get(nodeId);
                TaskInfo taskInfo = new TaskInfo();
                taskInfo.setTaskId(taskId);
                taskInfo.setNodeId(nodeId);
                taskInfos.add(taskInfo);
                tmpTasks.remove(new Integer(taskId));
                insertSameTask(taskId);
                balanced = calcBalance(nodeId);
                break;
            }
            if (tmpTasks.size() == 0 && !balanced)
                return ReturnCodeKeys.E014;
        }
        
        for (Integer time : sameTasks.keySet())
        {
            List<Integer> list = sameTasks.get(time);
            if (list.size() > 1)
            {
                
                List<TaskInfo> tasks = new ArrayList<TaskInfo>();
                for (Integer nodeId : taskStatus.keySet())
                {
                    List<TaskInfo> taskInfos = taskStatus.get(nodeId);
                    for (TaskInfo ti : taskInfos)
                    {
                        if (list.contains(ti.getTaskId()))
                        {
                            tasks.add(ti);
                        }
                    }
                }
                Collections.sort(tasks, comparatorByNodeId);
                Collections.sort(list);
                for (int i = 0; i < tasks.size(); i++)
                {
                    TaskInfo ti = tasks.get(i);
                    ti.setTaskId(list.get(i));
                }
            }
        }*/
        // 任务调度成功
        return ReturnCodeKeys.E013;
    }
    
    /**
     * 
     * 第七题，查询任务状态列表
     *
     * @author wangzejun
     * @param tasks
     * @return
     */
    public int queryTaskStatus(List<TaskInfo> tasks)
    {
        if(null == tasks)
        {
            return ReturnCodeKeys.E016;
        }
        for (Integer nodeId : taskStatus.keySet())
        {
            tasks.addAll(taskStatus.get(nodeId));
        }
        Collections.sort(tasks, comparator);
        System.out.println(tasks);
        // 查询成功
        return ReturnCodeKeys.E015;
    }
    
    // 被调用方法
    private int countTasks(List<TaskInfo> taskInfos)
    {
        int result = 0;
        for (TaskInfo taskInfo : taskInfos)
        {
            result = result + taskMap.get(taskInfo.getTaskId());
        }
        return result;
    }
    
    // 被调用方法
    private int findNode()
    {
        int tmpId = -1;
        int min = Integer.MAX_VALUE;
        for (Integer nodeId : nodes)
        {
            List<TaskInfo> taskInfos = taskStatus.get(nodeId);
            if (taskInfos == null)
            {
                return nodeId;
            }
            else
            {
                int w = countTasks(taskInfos);
                if (w < min)
                {
                    min = w;
                    tmpId = nodeId;
                }
            }
        }
        return tmpId;
    }
    
    // 被调用方法
    private boolean calcBalance(int nodeId)
    {
        int source = countTasks(taskStatus.get(nodeId));
        for (Integer id : nodes)
        {
            if (!id.equals(nodeId))
            {
                int t = 0;
                if (taskStatus.get(id) == null)
                {
                    t = 0;
                }
                else
                {
                    t = countTasks(taskStatus.get(id));
                }
                // 取绝对值比较
                if (Math.abs(t - source) > this.threshold)
                    return false;
            }
        }
        return true;
    }
    
    // 被调用方法
    private void insertSameTask(int taskId)
    {
        int time = taskMap.get(taskId);
        List<Integer> list = sameTasks.get(time);
        if (null == list)
        {
            list = new ArrayList<Integer>();
            sameTasks.put(time, list);
        }
        list.add(taskId);
    }
    
}
