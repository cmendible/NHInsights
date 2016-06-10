[![Build status](https://ci.appveyor.com/api/projects/status/q56t9cuo4g5ete37?svg=true)](https://ci.appveyor.com/project/cmendible/nhinsights)

# NHInsights

An **OracleManagedDataClientDriver** for **NHibernate** that will track the response times and success rates of your queries as dependencies using **Application Insights** Telemetry Client

Just change the driver in your config file and you are good to go!

    <hibernate-configuration xmlns="urn:nhibernate-configuration-2.2">
      <session-factory>
        ...
        <property name="connection.driver_class">NHInsights.OracleManagedDataClientDriver, NHInsights, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null</property>
        ...
      </session-factory>
    </hibernate-configuration>
