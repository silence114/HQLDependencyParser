package com.vdian.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Stack;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import static java.lang.System.exit;

public class HQLDependencyParser{

    private HashMap<String,ArrayList> inputTableList = new HashMap<String,ArrayList> ();
    private ArrayList<String> outputTableList = new ArrayList<String>();

    public HashMap<String,ArrayList> getInputTableList() { return inputTableList;}
    public ArrayList<String> getOutputTableList() { return outputTableList;}

    private String DEFAULT_DB = "default";
    /**
     * 解析query语句
     * @param query HQL查询语句
     */
    public void parseQuery(String query){
        query = query.trim();
        if("".equals(query) || null == query){return;}

        for(String sql : query.split(";")){
            sql = sql.trim().toLowerCase();
            if(sql.startsWith("set") || sql.startsWith("add") || sql.equals("") || sql == null) continue;
            if(sql.startsWith("use")){
                this.DEFAULT_DB = sql.replace("use","").trim();
                continue;
            }
            try{
                ParseDriver pd = new ParseDriver();
                ASTNode tree = pd.parse(sql);
                if(tree == null || tree.getChildCount() <= 1) continue;
                ASTNode child = (ASTNode)tree.getChild(0);
                System.out.println(child.dump());
                parseTree(child);
            }catch(ParseException ex){
                System.out.println(ex.getMessage());
                exit(0);
            }
        }
    }

    private void parseTree(ASTNode node){
        if (node.isNil()) return ;
        switch (node.getToken().getType()){
            case HiveParser.TOK_TABREF:
                if(HiveParser.TOK_TABNAME == ((ASTNode)node.getChild(0)).getToken().getType()){
                    parsetInput(node);
                }break;
            case HiveParser.TOK_DESTINATION:
                parseOutput(node);break;
            case HiveParser.TOK_INSERT_INTO:
                parseOutput(node);break;
            default:
                parseChildren(node);break;
        }
    }

    private void parsetInput(ASTNode node){
        String tableName = "";          //来源表表名
        ArrayList<String> columns = new ArrayList<String>();    //来源表的列名称

        ASTNode from = (ASTNode)node.getChild(0);
        ArrayList nodes = from.getChildren();
        if(nodes.size() == 1){
            tableName = this.DEFAULT_DB + "."+((ASTNode)nodes.get(0)).getText();
        }else if(nodes.size() == 2){
            tableName = ((ASTNode)nodes.get(0)).getText() + "."+((ASTNode)nodes.get(1)).getText();
        }

        ASTNode select = null;
        if("TOK_CTE".equals((node.getParent().getParent().getChild(0)).getText())
         || !"TOK_QUERY".equals((node.getParent().getParent()).getText())){
            return ;
        }else{
            select = (ASTNode)node.getParent().getParent().getChild(1).getChild(1);
        }

        for(Node n : select.getChildren()){
            ASTNode an = (ASTNode)n;
            if(HiveParser.TOK_SELEXPR == an.getToken().getType()){
                String col = parseColumn(an);
                if("" != col && null != col) columns.add(col);
            }
        }
        if(1 == columns.size() && "TOK_ALLCOLREF".equals(columns.get(0))){
            columns.clear();
            columns = getColumnsByTableName(tableName);
        }
        if(inputTableList.containsKey(tableName)){
            ArrayList<String> cols = inputTableList.get(tableName);
            columns.addAll(cols);
            HashSet<String> tmp = new HashSet<String>(columns);
            columns.clear();
            columns.addAll(tmp);
        }

        inputTableList.put(tableName,columns);
    }
    private String parseColumn(ASTNode node){
        Stack<ASTNode> nodes = new Stack<ASTNode>();

        for(Node n : node.getChildren()){
            nodes.push((ASTNode)n);
        }
        while(!nodes.isEmpty()){
            ASTNode n = nodes.pop();
            if(HiveParser.TOK_TABLE_OR_COL == n.getToken().getType()){
                return n.getChild(0).getText();
            }else if (HiveParser.TOK_ALLCOLREF == n.getToken().getType()){
                return "TOK_ALLCOLREF";
            }else if(n.getChildCount() > 0){
                for(Node nn : n.getChildren()){
                    nodes.push((ASTNode)nn);
                }
            }
        }
        return "";
    }
    private void parseOutput(ASTNode node){
        String tableName;
        if(node.getChildCount() >= 1 && HiveParser.TOK_TAB == ((ASTNode)node.getChild(0)).getToken().getType()){
            ArrayList nodes = ((ASTNode)node.getChild(0).getChild(0)).getChildren();

            if(nodes.size() == 1){
                tableName = this.DEFAULT_DB + "."+((ASTNode)nodes.get(0)).getText();
            }else if(nodes.size() == 2){
                tableName = ((ASTNode)nodes.get(0)).getText() + "."+((ASTNode)nodes.get(1)).getText();
            }else return;

            this.outputTableList.add(tableName);
        }
    }

    private void parseChildren(ASTNode node){
        if(node.getChildCount() <= 0) return;
        for(Node n : node.getChildren()){
            parseTree((ASTNode)n);
        }
    }

    //TODO 从元数据中查出表名对应的columns
    private ArrayList<String> getColumnsByTableName(String tableName){
        ArrayList<String> columns = new  ArrayList<String>();
        columns.add("col1");
        columns.add("col2");

        return columns;
    }
    public static void main(String[] args) throws IOException,ParseException,SemanticException {
        String query;
//        // 测试用例 1
//        String query ="use defal;insert overwrite table test1 select t1.id,t1.name,t1.age,t2.amount\n" +
//                "from(\n" +
//                "    select id,name,age from di.user_info\n" +
//                ")t1 join \n" +
//                "(\n" +
//                "    select user_id,amount from order_info\n" +
//                ")t2 on t1.id = t2.user_id;";

//        // 测试用例 2
         query = "insert into table test select * from di.user_info";

//        //测试用例 3
//         query = "select avg(age) as age_avg,min(age) as age_min from di.user_info where age > 10 group by name;";

//        //测试用例 4
//        query = "use alipaydw;\n" +
//                "insert into table test1\n" +
//                "select t1.id,t1.name,t1.age,t2.amount\n" +
//                "from(\n" +
//                "    select id,name,age from di.user_info\n" +
//                ")t1 join \n" +
//                "(\n" +
//                "    select user_id,amount from order_info\n" +
//                ")t2 on t1.id = t2.user_id;";

//        //测试用例 5
//         query = "insert overwrite table hzsearch.search_report_system partition (pt=\"2016-07-17\")\n" +
//                "select  /*+mapjoin(t3)*/\n" +
//                "        \"2016-07-17\" as stat_date\n" +
//                "        ,t1.keyword\n" +
//                "        ,t3.category\n" +
//                "        ,null as industry\n" +
//                "        ,t1.pv\n" +
//                "        ,t1.uv \n" +
//                "        ,(COALESCE(t2.ipv,0)/t1.pv) * 100 as ctr\n" +
//                "        ,0 as uv_order_tran\n" +
//                "        ,0 as cust_price_avg\n" +
//                "        ,0 as deal_amount\n" +
//                "        ,0 as pv_order_trans\n" +
//                "        ,0 as pay_order_cnt\n" +
//                "        ,0 as item_expose\n" +
//                "        ,0 as expose_ctr\n" +
//                "        ,0 as append_duy_user\n" +
//                "        ,0 as fav_cnt\n" +
//                "        ,0 as order_price_avg\n" +
//                "from \n" +
//                "(\n" +
//                "        select  lower(originalKeyword) as keyword\n" +
//                "               ,count(distinct userId) as uv \n" +
//                "               ,count(*) as pv\n" +
//                "          from  hzsearch.r_dd_log_app_vlog_atlasvitem\n" +
//                "         where  pt=\"2016-07-17\"\n" +
//                "           and  originalKeyword is not null\n" +
//                "           and  originalKeyword <> \"\"\n" +
//                "           and  catelist is not null\n" +
//                "           and  catelist <> \"\"\n" +
//                "         group  by originalKeyword\n" +
//                ")t1 \n" +
//                "left outer join\n" +
//                "(\n" +
//                "        SELECT  count(*) as ipv\n" +
//                "               ,lower(reverse(split( reverse(reqid),'_')[0])) AS keyword\n" +
//                "          FROM  di.wd_buyer_meta\n" +
//                "         WHERE  url_type='item'\n" +
//                "           and  enter_from='SEARCH'\n" +
//                "           AND  DAY='2016-07-17'\n" +
//                "         group  by lower(reverse(split( reverse(reqid),'_')[0]))\n" +
//                ")t2 on t1.keyword = t2.keyword\n" +
//                "left outer join(\n" +
//                "        select   lower(originalKeyword) as keyword\n" +
//                "                ,concat_ws('|', collect_set(level1_name)) as category\n" +
//                "        from\n" +
//                "        (\n" +
//                "              select   t1.originalKeyword\n" +
//                "                      ,t2.level1_name\n" +
//                "              from\n" +
//                "              (\n" +
//                "                      select  originalKeyword\n" +
//                "                             ,regexp_extract(cate,\"(\\\\d*):\\\\d*:[\\\\d|\\\\.]*\",1) as cate\n" +
//                "                      from \n" +
//                "                      (\n" +
//                "                            select  originalKeyword\n" +
//                "                                    ,split(regexp_replace(catelist,\"\\\\[|\\\\]\",\"\"),',') as catelist\n" +
//                "                             from  hzsearch.r_dd_log_app_vlog_atlasvitem\n" +
//                "                            where  pt=\"2016-07-17\"\n" +
//                "                              and  originalKeyword is not null\n" +
//                "                              and  originalKeyword <> \"\" \n" +
//                "                      ) t1 LATERAL VIEW explode(t1.catelist) adTable AS cate\n" +
//                "                      where length(regexp_extract(cate,\"(\\\\d*):\\\\d*:[\\\\d|\\\\.]*\",1)) > 0\n" +
//                "                      group by originalKeyword,regexp_extract(cate,\"(\\\\d*):\\\\d*:[\\\\d|\\\\.]*\",1)\n" +
//                "               ) t1 join \n" +
//                "               (\n" +
//                "                      select level1_id,level1_name \n" +
//                "                        from di.taobao_category \n" +
//                "                       group by level1_id,level1_name\n" +
//                "               )t2 on t1.cate = t2.level1_id\n" +
//                "        ) t2 \n" +
//                "        group by originalKeyword\n" +
//                ")t3 on t1.keyword = t3.keyword\n" +
//                "order by t1.pv desc;";


//        query = "with t1 as (\n" +
//                "    select id,name,age from user_info where age > 10\n" +
//                ")\n" +
//                "insert into table test_tab\n" +
//                "select \n" +
//                "    t2.*,t3.address\n" +
//                "from\n" +
//                "(\n" +
//                "    select t1.id,t1.name,t1.age,t2.amount \n" +
//                "    from t1 join \n" +
//                "    (\n" +
//                "        select user_id,amount from hzsearch.order_info\n" +
//                "    )t2 on t1.id = t2.user_id\n" +
//                ")t2 join\n" +
//                "(\n" +
//                "    select t1.id,t2.address\n" +
//                "    from t1 join\n" +
//                "    (\n" +
//                "        select user_id,address from user_extend\n" +
//                "    )t2 on t1.id = t2.user_id\n" +
//                ")t3 on t2.id = t3.id";

//        query = "with q1 as ( select key from src where key = '4') select * from q1;";

        HQLDependencyParser lep = new HQLDependencyParser();
        lep.parseQuery(query);

        System.out.println("---------input----------");
        HashMap<String,ArrayList> input = lep.getInputTableList();
        for(String tableName : input.keySet()){
            System.out.print(tableName);
            System.out.println(input.get(tableName));
        }

        System.out.println("---------output----------");
        ArrayList<String> output = lep.getOutputTableList();
        for(String tableName : output){
            System.out.print(tableName);
        }


    }
}