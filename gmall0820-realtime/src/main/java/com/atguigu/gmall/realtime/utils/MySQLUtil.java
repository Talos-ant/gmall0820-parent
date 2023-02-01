package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO
 *
 * @Description
 * @Author talos
 * @Date 2023/2/1 4:56 PM
 **/
public class MySQLUtil {

    /**
     *
     * @param sql
     * @param clz 映射类的对象
     * @param underScoreToCamel 下划线转化
     * @return
     * @param <T> 定义泛型模板
     */
    public static <T> List<T> queryList(String sql,Class<T> clz,boolean underScoreToCamel){

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //建立连接
            conn = DriverManager.getConnection(
                    "jdbc:mysql://hadoop202:3306/gmall2021_realtime?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456");
            //创建数据库操作对象
            ps =  conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();
            //处理结果集
            ResultSetMetaData md = rs.getMetaData();
            //声明集合对象，用于封装返回结果
            List<T> resultList = new ArrayList<T>();

            //每循环一次，获取一条查询结果
            while (rs.next()) {
                //通过反射创建要将查询结果转换为目标类型的对象
                T obj = clz.newInstance();
                //对查询出的列进行遍历，每遍历一次得到一个列名
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    String columnName = md.getColumnName(i);
                    String propertyName = "";
                    //如果开启了下划线转驼峰的映射，那么将列名里的下划线转换为属性的打
                    if (underScoreToCamel) {
                        //直接调用Google的guava的CaseFormat  LOWER_UNDERSCORE小写开头+下划线->LOWER_CAMEL小写开头+驼峰
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //调用apache的commons-bean中的工具类，给Bean的属性赋值
                    BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                }
                resultList.add(obj);
            }
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询mysql失败！");
        }finally {
            //释放资源
            if(rs!=null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(ps!=null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(conn!=null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 测试验证
     * @param args
     */
    public static void main(String[] args) {
        List<TableProcess> tableProcesses = queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }
    }
}
