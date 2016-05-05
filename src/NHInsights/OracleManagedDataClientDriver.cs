namespace NHInsights
{
    using System.Data;
    using System.Data.Common;
    using NHibernate.AdoNet;
    using NHibernate.Driver;
    using NHibernate.Engine.Query;
    using NHibernate.SqlTypes;
    using NHibernate.Util;
    using NHInsights.Infrastructure;

    public class OracleManagedDataClientDriver : ReflectionBasedDriver, IEmbeddedBatcherFactoryProvider
    {
        private const string driverAssemblyName = "Oracle.ManagedDataAccess";

		private const string connectionTypeName = "Oracle.ManagedDataAccess.Client.OracleConnection";

        private const string commandTypeName = "NHInsights.Infrastructure.InsightsDbCommand";

		private static readonly SqlType GuidSqlType = new SqlType(DbType.Binary, 16);

		private readonly System.Reflection.PropertyInfo oracleCommandBindByName;

		private readonly System.Reflection.PropertyInfo oracleDbType;

		private readonly object oracleDbTypeRefCursor;

		private readonly object oracleDbTypeXmlType;

		private readonly object oracleDbTypeBlob;

		/// <summary></summary>
		public override string NamedPrefix
		{
			get
			{
				return ":";
			}
		}

		/// <summary></summary>
		public override bool UseNamedPrefixInParameter
		{
			get
			{
				return true;
			}
		}

		/// <summary></summary>
		public override bool UseNamedPrefixInSql
		{
			get
			{
				return true;
			}
		}

		System.Type IEmbeddedBatcherFactoryProvider.BatcherFactoryClass
		{
			get
			{
				return typeof(OracleDataClientBatchingBatcherFactory);
			}
		}

		/// <summary>
		/// Initializes a new instance of <see cref="T:NHibernate.Driver.OracleDataClientDriver" />.
		/// </summary>
		/// <exception cref="T:NHibernate.HibernateException">
		/// Thrown when the <c>Oracle.ManagedDataAccess</c> assembly can not be loaded.
		/// </exception>
        public OracleManagedDataClientDriver()
            : base("Oracle.ManagedDataAccess.Client", "Oracle.ManagedDataAccess", "Oracle.ManagedDataAccess.Client.OracleConnection", "NHInsights.Infrastructure.InsightsDbCommand")
		{
            System.Type type = ReflectHelper.TypeFromAssembly("NHInsights.Infrastructure.InsightsDbCommand", "NHInsights", true);
			this.oracleCommandBindByName = type.GetProperty("BindByName");
			System.Type type2 = ReflectHelper.TypeFromAssembly("Oracle.ManagedDataAccess.Client.OracleParameter", "Oracle.ManagedDataAccess", true);
			this.oracleDbType = type2.GetProperty("OracleDbType");
			System.Type enumType = ReflectHelper.TypeFromAssembly("Oracle.ManagedDataAccess.Client.OracleDbType", "Oracle.ManagedDataAccess", true);
			this.oracleDbTypeRefCursor = System.Enum.Parse(enumType, "RefCursor");
			this.oracleDbTypeXmlType = System.Enum.Parse(enumType, "XmlType");
			this.oracleDbTypeBlob = System.Enum.Parse(enumType, "Blob");
		}

		/// <remarks>
		/// This adds logic to ensure that a DbType.Boolean parameter is not created since
		/// ODP.NET doesn't support it.
		/// </remarks>
		protected override void InitializeParameter(IDbDataParameter dbParam, string name, SqlType sqlType)
		{
			DbType dbType = sqlType.DbType;
			switch (dbType)
			{
			case DbType.Binary:
				this.InitializeParameter(dbParam, name, this.oracleDbTypeBlob);
				return;
			case DbType.Byte:
				break;
			case DbType.Boolean:
				base.InitializeParameter(dbParam, name, SqlTypeFactory.Int16);
				return;
			default:
				if (dbType == DbType.Guid)
				{
                    base.InitializeParameter(dbParam, name, OracleManagedDataClientDriver.GuidSqlType);
					return;
				}
				if (dbType == DbType.Xml)
				{
					this.InitializeParameter(dbParam, name, this.oracleDbTypeXmlType);
					return;
				}
				break;
			}
			base.InitializeParameter(dbParam, name, sqlType);
		}

		private void InitializeParameter(IDbDataParameter dbParam, string name, object sqlType)
		{
			dbParam.ParameterName = base.FormatNameForParameter(name);
			this.oracleDbType.SetValue(dbParam, sqlType, null);
		}

		protected override void OnBeforePrepare(IDbCommand command)
		{
			base.OnBeforePrepare(command);
			this.oracleCommandBindByName.SetValue(command, true, null);
			CallableParser.Detail detail = CallableParser.Parse(command.CommandText);
			if (!detail.IsCallable)
			{
				return;
			}
			command.CommandType = CommandType.StoredProcedure;
			command.CommandText = detail.FunctionName;
			this.oracleCommandBindByName.SetValue(command, false, null);
			IDbDataParameter dbDataParameter = command.CreateParameter();
			this.oracleDbType.SetValue(dbDataParameter, this.oracleDbTypeRefCursor, null);
			dbDataParameter.Direction = (detail.HasReturn ? ParameterDirection.ReturnValue : ParameterDirection.Output);
			command.Parameters.Insert(0, dbDataParameter);
		}

        public override IDbCommand CreateCommand()
        {
            var command = base.CreateCommand();

            command = new InsightsDbCommand((DbCommand)command, (DbConnection)command.Connection) as IDbCommand;

            return command;
        }
    }
}