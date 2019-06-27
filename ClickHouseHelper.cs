//=====================================================
//GUID: 84313B54-9C53-49AC-BC81-A48CFAEFF5DC
//MachineName: Administrator
//FileName: ClickHouseContext
//Creator: Shaojianan
//Contact:1542873057@qq.com
//CreateTime: 2019-06-14 15:16:09
//======================================================

using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading;
using ClickHouse.Ado;
using NetTopologySuite.IO;

namespace Kafka2ClickHouse.Core
{
    public class ClickHouseHelper : IDisposable
    {
        private readonly string _connectionString = "Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=127.0.0.1;Port=9000;Database=db;User=sa;Password=123456";

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

        public int ExecuteNoQuery(string sql, CommandType commandType, params ClickHouseParameter[] parameters)
        {
            int result = 0;
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
                result = command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return result;
        }

        public int ExecuteNoQuery(string sql, params ClickHouseParameter[] parameters)
        {
            int result = 0;
            try
            {
                result = ExecuteNoQuery(sql, CommandType.Text, parameters);
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return result;
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

        public (ulong, DataTable) ExecuteDataTableByPagination(string sql, int pageindex, int pagesize)
        {
            DataTable result;
            ulong totalCount = 0;
            try
            {
                (string countsql, string pagesql) = GetCountAndPageSql(sql, pageindex, pagesize);
                result = ExecuteDataTable(pagesql, CommandType.Text);
                totalCount = ExecuteScalar<ulong>(countsql);
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return (totalCount, result);
        }

        public (ulong, List<T>) ExecuteListByPagination<T>(string sql, int pageindex, int pagesize) where T : class
        {
            List<T> result;
            ulong totalCount = 0;
            try
            {
                (string countsql, string pagesql) = GetCountAndPageSql(sql, pageindex, pagesize);
                result = ExecuteList<T>(pagesql, CommandType.Text);
                totalCount = ExecuteScalar<ulong>(countsql);
            }
            catch (Exception e)
            {
                this.Dispose();
                throw;
            }

            return (totalCount, result);
        }

        #region private
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