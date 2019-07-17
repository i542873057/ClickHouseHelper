using System;
using System.Collections.Generic;
using System.Data;
using ClickhouseDemo.ClickHouseHelperSrc;

namespace ClickhouseDemo
{
    class Program
    {
        static ClickHouseHelper dbHelper = new ClickHouseHelper();
        static void Main(string[] args)
        {
            //ListDemo();
            //ListPaginationDemo();
            BulkInsertDemo();
            Console.WriteLine("Finished!");
            Console.ReadKey();
        }

        public static void ListDemo()
        {
            List<Test_Log> logs = dbHelper.ExecuteList<Test_Log>("select * from its.test_log");
            logs.ForEach(u => Console.WriteLine(u.sys_name));
        }

        public static void ListPaginationDemo()
        {
            (ulong totalCount, List<Test_Log> logs) = dbHelper.ExecuteListByPagination<Test_Log>("select * from its.test_log", 1, 3);
            Console.WriteLine($"总条数为：{totalCount}");
            logs.ForEach(u => Console.WriteLine(u.sys_name));
        }

        public static void BulkInsertDemo()
        {
            List<Test_Log> list = new List<Test_Log>
            {
                new Test_Log{sys_id = 101,sys_code = "aaa",sys_name = "ccc"},
                new Test_Log{sys_id = 102,sys_code = "ccc",sys_name = "ddd"},
                new Test_Log{sys_id = 103,sys_code = "ddd",sys_name = "eee"},
            };
            dbHelper.BulkInsert(list, "test_log");
            Console.WriteLine($"操作成功");
        }
    }

    public class Test_Log
    {
        public double sys_id { get; set; }
        public string sys_code { get; set; }
        public string sys_name { get; set; }
    }
}
