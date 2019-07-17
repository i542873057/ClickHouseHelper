//=====================================================
//GUID: 6E2CC947-D1DB-46A9-B340-F402143EA9B4
//MachineName: Administrator
//FileName: ClickHouseHelper
//Creator: Shaojianan
//Contact:1542873057@qq.com
//CreateTime: 2019-07-17 08:47:19
//======================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using ClickHouse.Ado;

namespace ClickhouseDemo.ClickHouseHelperSrc
{
    public class ClickHouseHelper : IDisposable
    {
        private readonly string _connectionString = "Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=127.0.0.1;Port=9000;Database=db;User=default;Password=123456";

        private ClickHouseConnection _clickHouseConnection;

        #region Constructor

        public ClickHouseHelper()
        {
            this.CreateConnection();
        }

        public ClickHouseHelper(string connectionString) : this()
        {
            this._connectionString = connectionString;
        }

        #endregion

        public ClickHouseConnection CreateConnection()
        {
            if (_clickHouseConnection == null)
            {
                var settings = new ClickHouseConnectionSettings(_connectionString);
                var cnn = new ClickHouseConnection(settings);
                if (cnn.State != ConnectionState.Open)
                {
                    cnn.Open();
                }
                _clickHouseConnection = cnn;
            }
            return _clickHouseConnection;
        }

        public void ExecuteNoQuery(string sql, CommandType commandType, params ClickHouseParameter[] parameters)
        {
            try
            {
                if (_clickHouseConnection == null)
                {
                    this.CreateConnection();
                }
                var command = _clickHouseConnection.CreateCommand();
                command.CommandText = sql;
                command.CommandType = commandType;
                AttachParameters(command.Parameters, parameters);
                command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }
        }

        public void ExecuteNoQuery(string sql, params ClickHouseParameter[] parameters)
        {
            try
            {
                ExecuteNoQuery(sql, CommandType.Text, parameters);
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }
        }

        public T ExecuteScalar<T>(string sql, CommandType commandType, params ClickHouseParameter[] parameters)
        {
            T result;
            try
            {
                if (_clickHouseConnection == null)
                {
                    this.CreateConnection();
                }
                var command = _clickHouseConnection.CreateCommand();
                command.CommandText = sql;
                command.CommandType = commandType;
                AttachParameters(command.Parameters, parameters);
                result = (T)command.ExecuteScalar();
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return result;
        }

        public T ExecuteScalar<T>(string sql, params ClickHouseParameter[] parameters)
        {
            T result;
            try
            {
                result = ExecuteScalar<T>(sql, CommandType.Text, parameters);
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return result;
        }

        public IDataReader ExecuteReader(string sql, CommandType commandType, params ClickHouseParameter[] parameters)
        {
            IDataReader result = null;
            try
            {
                if (_clickHouseConnection == null)
                {
                    this.CreateConnection();
                }
                var command = _clickHouseConnection.CreateCommand();
                command.CommandText = sql;
                command.CommandType = commandType;
                AttachParameters(command.Parameters, parameters);
                result = command.ExecuteReader();
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return result;
        }

        public IDataReader ExecuteReader(string sql, params ClickHouseParameter[] parameters)
        {
            IDataReader result;
            try
            {
                result = ExecuteReader(sql, CommandType.Text, parameters);
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return result;
        }

        /// <summary>
        /// 执行sql返回一个DataTable
        /// </summary>
        /// <param name="sql">sql语句</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="parameters">sql参数</param>
        /// <returns></returns>
        public DataTable ExecuteDataTable(string sql, CommandType commandType, params ClickHouseParameter[] parameters)
        {
            DataTable result = null;
            try
            {
                var dataReader = ExecuteReader(sql, commandType, parameters);
                if (dataReader != null)
                {
                    result = DataReaderToDataTable(dataReader);
                }
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return result;
        }

        /// <summary>
        /// 执行sql返回一个DataTable
        /// </summary>
        /// <param name="sql">sql语句</param>
        /// <param name="parameters">sql参数</param>
        /// <returns></returns>
        public DataTable ExecuteDataTable(string sql, params ClickHouseParameter[] parameters)
        {
            DataTable result;
            try
            {
                result = ExecuteDataTable(sql, CommandType.Text, parameters);
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return result;
        }

        /// <summary>
        /// 执行sql返回指定类型的List
        /// </summary>
        /// <typeparam name="T">需要返回的类型</typeparam>
        /// <param name="sql">sql语句</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="parameters">sql参数</param>
        /// <returns></returns>
        public List<T> ExecuteList<T>(string sql, CommandType commandType, params ClickHouseParameter[] parameters) where T : class
        {
            List<T> resultList = new List<T>();
            try
            {
                var dataReader = ExecuteReader(sql, commandType, parameters);
                if (dataReader != null)
                {
                    resultList = ReaderToList<T>(dataReader);
                }
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return resultList;
        }

        /// <summary>
        /// 执行sql返回指定类型的List
        /// </summary>
        /// <typeparam name="T">需要返回的类型</typeparam>
        /// <param name="sql">sql语句</param>
        /// <param name="parameters">sql参数</param>
        /// <returns></returns>
        public List<T> ExecuteList<T>(string sql, params ClickHouseParameter[] parameters) where T : class
        {
            List<T> resultList = new List<T>();
            try
            {
                resultList = ExecuteList<T>(sql, CommandType.Text, parameters);
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return resultList;

        }

        /// <summary>
        /// DataTable分页;注：传入的sql请自己增加排序条件
        /// </summary>
        /// <param name="sql">sql语句</param>
        /// <param name="pageindex">页码</param>
        /// <param name="pagesize">每页条数</param>
        /// <param name="parameters">sql参数</param>
        /// <returns>返回总条数和分页后数据</returns>
        public (ulong, DataTable) ExecuteDataTableByPagination(string sql, int pageindex, int pagesize, params ClickHouseParameter[] parameters)
        {
            DataTable result;
            ulong totalCount = 0;
            try
            {
                (string countsql, string pagesql) = GetCountAndPageSql(sql, pageindex, pagesize);
                result = ExecuteDataTable(pagesql, CommandType.Text, parameters);
                totalCount = ExecuteScalar<ulong>(countsql);
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return (totalCount, result);
        }

        /// <summary>
        /// List分页;注：传入的sql请自己增加排序条件
        /// </summary>
        /// <typeparam name="T">需要返回的list类型</typeparam>
        /// <param name="sql">sql语句</param>
        /// <param name="pageindex">页码</param>
        /// <param name="pagesize">每页条数</param>
        /// <param name="parameters">sql参数</param>
        /// <returns>返回总条数和分页后数据</returns>
        public (ulong, List<T>) ExecuteListByPagination<T>(string sql, int pageindex, int pagesize, params ClickHouseParameter[] parameters) where T : class
        {
            List<T> result;
            ulong totalCount = 0;
            try
            {
                (string countsql, string pagesql) = GetCountAndPageSql(sql, pageindex, pagesize);
                result = ExecuteList<T>(pagesql, CommandType.Text, parameters);
                totalCount = ExecuteScalar<ulong>(countsql);
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return (totalCount, result);
        }

        /// <summary>
        /// 批量新增数据;注：单条增加请使用ExecuteNonQuery
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="sourceList">源数据</param>
        /// <param name="tbName">需要插入的表名;注：不填默认为类名</param>
        public void BulkInsert<T>(List<T> sourceList, string tbName = "") where T : class
        {
            tbName = string.IsNullOrEmpty(tbName) ? typeof(T).Name : tbName;
            try
            {
                string insertClickHouseSql = $"INSERT INTO {tbName} ({GetColumns<T>()}) VALUES @bulk;";
                if (_clickHouseConnection == null)
                {
                    this.CreateConnection();
                }
                var command = _clickHouseConnection.CreateCommand();
                command.CommandText = insertClickHouseSql;
                command.Parameters.Add(new ClickHouseParameter
                {
                    ParameterName = "bulk",
                    Value = List2AList(sourceList)
                });
                command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }
        }

        #region private

        private List<dynamic[]> List2AList<T>(List<T> sourceList)
        {
            List<dynamic[]> result = new List<dynamic[]>();
            sourceList.ForEach(u =>
            {
                var dic = GetColumnsAndValue(u);
                result.Add(dic.Select(i => i.Value).ToArray());
            });
            return result;
        }

        private string GetColumns<T>()
        {
            try
            {
                var dic = GetColumnsAndValue<T>(default(T));
                return string.Join(",", dic.Select(u => u.Key).ToArray());
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }
        }

        private Dictionary<string, object> GetColumnsAndValue<T>(T u)
        {
            try
            {
                Dictionary<string, object> dic = new Dictionary<string, object>();
                Type t = typeof(T);
                if (u != null)
                {
                    t = u.GetType();
                }
                var columns = t.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                foreach (var item in columns)
                {
                    object v = null;
                    if (u != null)
                    {
                        v = item.GetValue(u);
                    }
                    dic.TryAdd(item.Name, v);
                }

                return dic;
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }
        }

        private void AttachParameters(ClickHouseParameterCollection parametersCollection, ClickHouseParameter[] parameters)
        {
            foreach (var item in parameters)
            {
                parametersCollection.Add(item);
            }
        }

        /// <summary>
        ///  将IDataReader转换为DataTable
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        private static DataTable DataReaderToDataTable(IDataReader reader)
        {
            DataTable objDataTable = new DataTable("Table");
            int intFieldCount = reader.FieldCount;
            for (int intCounter = 0; intCounter < intFieldCount; ++intCounter)
            {
                objDataTable.Columns.Add(reader.GetName(intCounter).ToUpper(), reader.GetFieldType(intCounter));
            }
            objDataTable.BeginLoadData();
            object[] objValues = new object[intFieldCount];
            while (reader.NextResult())
            {
                while (reader.Read())
                {
                    reader.GetValues(objValues);
                    objDataTable.LoadDataRow(objValues, true);
                }
            }
            reader.Close();
            objDataTable.EndLoadData();
            return objDataTable;
        }

        private static T ReaderToModel<T>(IDataReader dr)
        {
            try
            {
                using (dr)
                {
                    if (dr.Read())
                    {
                        List<string> list = new List<string>(dr.FieldCount);
                        for (int i = 0; i < dr.FieldCount; i++)
                        {
                            list.Add(dr.GetName(i).ToLower());
                        }
                        T model = Activator.CreateInstance<T>();
                        foreach (PropertyInfo pi in model.GetType().GetProperties(BindingFlags.GetProperty | BindingFlags.Public | BindingFlags.Instance))
                        {
                            if (list.Contains(pi.Name.ToLower()))
                            {
                                if (!IsNullOrDBNull(dr[pi.Name]))
                                {
                                    pi.SetValue(model, HackType(dr[pi.Name], pi.PropertyType), null);
                                }
                            }
                        }
                        return model;
                    }
                }
                return default(T);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private static List<T> ReaderToList<T>(IDataReader dr)
        {
            using (dr)
            {
                List<string> field = new List<string>(dr.FieldCount);
                for (int i = 0; i < dr.FieldCount; i++)
                {
                    field.Add(dr.GetName(i).ToLower());
                }
                List<T> list = new List<T>();
                while (dr.NextResult())
                {
                    while (dr.Read())
                    {
                        T model = Activator.CreateInstance<T>();
                        foreach (PropertyInfo property in model.GetType().GetProperties(BindingFlags.GetProperty | BindingFlags.Public | BindingFlags.Instance))
                        {
                            if (field.Contains(property.Name.ToLower()))
                            {
                                if (!IsNullOrDBNull(dr[property.Name]))
                                {
                                    property.SetValue(model, HackType(dr[property.Name], property.PropertyType), null);
                                }
                            }
                        }
                        list.Add(model);
                    }
                }
                return list;
            }
        }

        //这个类对可空类型进行判断转换，要不然会报错
        private static object HackType(object value, Type conversionType)
        {
            if (conversionType.IsGenericType && conversionType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
            {
                if (value == null)
                    return null;

                System.ComponentModel.NullableConverter nullableConverter = new System.ComponentModel.NullableConverter(conversionType);
                conversionType = nullableConverter.UnderlyingType;
            }
            return Convert.ChangeType(value, conversionType);
        }

        private static bool IsNullOrDBNull(object obj)
        {
            return ((obj is DBNull) || string.IsNullOrEmpty(obj.ToString())) ? true : false;
        }

        private (string, string) GetCountAndPageSql(string sql, int pageindex, int pagesize)
        {
            string countSql = $"SELECT COUNT(1) count FROM ({sql}) A";
            string pageSql = $"select * from ({sql}) LIMIT {pagesize} OFFSET {(pageindex - 1) * pagesize}";
            return (countSql, pageSql);
        }


        public void Dispose()
        {
            _clickHouseConnection?.Dispose();
            _clickHouseConnection = null;
            GC.Collect();
        }
        #endregion
    }
}