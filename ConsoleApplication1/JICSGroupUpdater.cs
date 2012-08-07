using System;
using System.Collections;
using System.Data;
using System.DirectoryServices;
using System.Xml;
//using CUS.OdbcConnectionClass2;  // cannot use Jenzabar's because of "private" GetERPConnectionString
using System.Data.Odbc;
//using Jenzabar.ICS;
//using Jenzabar.Portal.Framework.Data;

namespace JICSGroupUpdater
{
	/// <summary>
	/// Summary description for Class1.
	/// </summary>
	class JICSGroupUpdater
	{
		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static void Main(string[] args)
		{
			Configs configs;
			try 
			{
				configs = new Configs("../ClientConfig/JICSGroupUpdater.config");
			}
			catch (Exception ex)
			{
				Console.WriteLine("ERROR - BAD CONFIG FILE: "+ex.Message);
				return;
			}

			string dbConnection  = configs.configValue("dbconnection");
			string adamAddress   = configs.configValue("adamaddress");
			string loginSubquery = configs.configValue("loginsubquery");
			string groupName;
			string memberSubquery;

			Console.WriteLine("dbConnection="+dbConnection); Console.WriteLine();
			Console.WriteLine("adamAddress="+adamAddress); Console.WriteLine();
			Console.WriteLine("loginSubquery="+loginSubquery); Console.WriteLine();
			Console.WriteLine("(members, below, includes leaders.  this script does not add or remove leaders.)"); Console.WriteLine();

			XmlNodeList memberSubqueryList = configs.configValues("membersubquery");
			XmlAttributeCollection attributes;

			if (memberSubqueryList == null) 
			{
				Console.WriteLine("WARNING: Missing <membersubquery> element in config file.");
			} 
			else 
			{
				foreach (XmlNode node in memberSubqueryList)
				{	
					node.Normalize();
					attributes = node.Attributes;
					groupName = attributes["groupname"].Value;
					memberSubquery = node.InnerText;
					Console.WriteLine();
					Console.WriteLine("groupName="+groupName);
					Console.WriteLine("memberSubquery="+memberSubquery);

					try 
					{
						CampusGroupAuto campusGroup =
							new CampusGroupAuto(groupName,memberSubquery,loginSubquery,"../ClientConfig/"+dbConnection,adamAddress);
						Console.WriteLine("members: "+campusGroup.Count());
						Console.WriteLine("added: "+campusGroup.AddNewMembers());
						Console.WriteLine("removed: "+campusGroup.RemoveDefunctMembers());
						Console.WriteLine("members: "+campusGroup.Count());
					}
					catch (Exception ex)
					{
						Console.WriteLine("WARNING: "+ex.Message);
					}
					Console.WriteLine();
				}
			}
		}

		public class Configs
		{
			XmlDocument xmlDoc;

			public Configs(string configFile)
			{
				this.xmlDoc = new XmlDocument();
				xmlDoc.Load(configFile);
			}

			public string configValue(string configName)
			{
				XmlNodeList values = this.xmlDoc.GetElementsByTagName(configName);
				if (values.Count == 0) 
				{
					return null;
				} 
				else 
				{
					return values[values.Count-1].InnerText.Trim();
				}
			}

			public XmlNodeList configValues(string configName)
			{
				XmlNodeList values = this.xmlDoc.GetElementsByTagName(configName);
				if (values.Count == 0) return null;

				return values;
			}
		}

		public class CampusGroupAuto
		{
			string groupName;
			string groupNameEscaped;
			string memberSubquery;
			string loginSubquery;
			string dbConnection;
			string adamAddress;
			string memberCN;
			string leaderCN;

			public CampusGroupAuto(string groupName, string memberSubquery, string loginSubquery, string dbConnection, string adamAddress)
			{
				this.groupName = groupName;
				this.groupNameEscaped = groupName.Replace("'","''");
				this.memberSubquery = memberSubquery;
				this.loginSubquery = loginSubquery;
				this.dbConnection = dbConnection;
				this.adamAddress = adamAddress;
				this.memberCN = "CN=Member,OU=Groups,CN="+this.groupName+
                                  ",CN=Campus Groups,CN=Student Life,CN=Portal,O=Jenzabar,C=US";
				this.leaderCN = "CN=Leader,OU=Groups,CN="+this.groupName+
                                  ",CN=Campus Groups,CN=Student Life,CN=Portal,O=Jenzabar,C=US";

				// check for dbConnection being ok and that the campus group exists
				Exception ex = null;
				OdbcConnectionClassX odbcConn = new OdbcConnectionClassX(dbConnection);
				string sql = @"select CampusGroupID from ICS_CampusGroup as cg
								where cg.Name='"+this.groupName.Replace("'","''")+@"'";
				DataTable dt = odbcConn.ConnectToERP(sql,ref ex);
				if (ex != null) throw new ApplicationException(ex.Message + " " + sql);
				if (dt.Rows.Count == 0) throw new ApplicationException("Campus Group '"+this.groupName+"' not defined in JICS yet.");
			}

			public int AddNewMembers()
			{
				Exception ex = null;
				OdbcConnectionClassX odbcConn = new OdbcConnectionClassX(this.dbConnection);
				string sql = @"select member.hostid, login.login
								from (@@membersubquery) as member
								join FWK_User as u on u.HostID=member.hostid
								join (@@loginsubquery) as login on login.hostid=u.hostid
								left outer join 
									(select UserID
									from ICS_CampusGroupUser as cgu
									join ICS_CampusGroup as cg on cg.CampusGroupID=cgu.CampusGroupID
									where cg.Name='@@groupname') as om on om.UserID=u.ID
								where om.UserID is null";
				sql=sql.Replace("@@membersubquery",this.memberSubquery);
				sql=sql.Replace("@@loginsubquery",this.loginSubquery);
				sql=sql.Replace("@@groupname",this.groupNameEscaped);

				DataTable dt = odbcConn.ConnectToERP(sql,ref ex);
				if (ex != null) throw new ApplicationException(ex.Message + " " + sql);

				DirectoryEntry member = new DirectoryEntry("LDAP://"+this.adamAddress+"/"+this.memberCN);
				PropertyValueCollection memberProperty = member.Properties["member"];

				DirectoryEntry leader = new DirectoryEntry("LDAP://"+this.adamAddress+"/"+this.leaderCN);
				PropertyValueCollection leaderProperty = leader.Properties["member"];

				string newHostIDs = "";
				int count = 0;
				
				foreach (DataRow dr in dt.Rows) 
				{   
					string loginCN = "CN="+dr["login"]+",OU=PortalUsers,CN=Portal,O=Jenzabar,C=US";
					if (!leaderProperty.Contains(loginCN))
					{
						memberProperty.Add(loginCN);
						Console.WriteLine(dr["hostid"].ToString().Trim());
						newHostIDs = newHostIDs + "," + dr["hostid"].ToString().Trim();
						count++;
					}
				}

				if (newHostIDs != "")
				{
					newHostIDs = newHostIDs.Remove(0,1);  // remove the initial comma
					string insertSql = @"INSERT INTO ICS_CampusGroupUser (CampusGroupID, UserID, StartDate, Status)
											SELECT     cg.CampusGroupID, FWK_User.ID, GETDATE() AS StartDate, 0 AS Status
											FROM         ICS_CampusGroup AS cg
											LEFT OUTER JOIN FWK_User ON FWK_User.HostID IN (@@newhostids)
											WHERE     (cg.Name = '@@groupname')";
					insertSql=insertSql.Replace("@@newhostids",newHostIDs);
					insertSql=insertSql.Replace("@@groupname",this.groupNameEscaped);
					dt = odbcConn.ConnectToERP(insertSql,ref ex);
					if (ex != null) throw new ApplicationException(ex.Message + " " + insertSql);
				}

				member.CommitChanges();
				return count;				
			}

			public int RemoveDefunctMembers()
			{
				Exception ex = null;
				OdbcConnectionClassX odbcConn = new OdbcConnectionClassX(this.dbConnection);
				string sql = @"select u.hostid, login.login
								from ICS_CampusGroupUser AS cgu
								join ICS_CampusGroup AS cg ON cg.CampusGroupID = cgu.CampusGroupID and cg.Name = '@@groupname'
								join FWK_User AS u ON u.ID = cgu.UserID
								join (@@loginsubquery) as login on login.hostid=u.hostid
								left outer join (@@membersubquery) AS member ON member.hostid = u.HostID
								where (member.hostid IS NULL)";
				sql=sql.Replace("@@membersubquery",this.memberSubquery);
				sql=sql.Replace("@@loginsubquery",this.loginSubquery);
				sql=sql.Replace("@@groupname",this.groupNameEscaped);

				DataTable dt = odbcConn.ConnectToERP(sql,ref ex);
				if (ex != null) throw new ApplicationException(ex.Message + " " + sql);

				DirectoryEntry member = new DirectoryEntry("LDAP://"+this.adamAddress+"/"+this.memberCN);
				PropertyValueCollection memberProperty = member.Properties["member"];

				DirectoryEntry leader = new DirectoryEntry("LDAP://"+this.adamAddress+"/"+this.leaderCN);
				PropertyValueCollection leaderProperty = leader.Properties["member"];

				string defunctHostIDs = "";
				int count = 0;
				
				foreach (DataRow dr in dt.Rows) 
				{   
					string loginCN = "CN="+dr["login"]+",OU=PortalUsers,CN=Portal,O=Jenzabar,C=US";
					if (!leaderProperty.Contains(loginCN))
					{
						memberProperty.Remove(loginCN);
						Console.WriteLine(dr["hostid"].ToString().Trim());
						defunctHostIDs = defunctHostIDs + "," + dr["hostid"].ToString().Trim();
						count++;
					}
				}
				
				if (defunctHostIDs != "")
				{
					defunctHostIDs = defunctHostIDs.Remove(0,1);  // remove the initial comma
					string deleteSql = @"delete from ICS_CampusGroupUser
											where ICS_CampusGroupUser.CampusGroupUserID in
												(select CampusGroupUserID
												from ICS_CampusGroupUser as cgu
													join ICS_CampusGroup as cg on cgu.CampusGroupID = cg.CampusGroupID
													join fwk_user as u on u.id = cgu.userid
												where cg.Name='@@groupname'
													and u.hostid in (@@defuncthostids))";
					deleteSql=deleteSql.Replace("@@defuncthostids",defunctHostIDs);
					deleteSql=deleteSql.Replace("@@groupname",this.groupNameEscaped);
					dt = odbcConn.ConnectToERP(deleteSql,ref ex);
					if (ex != null) throw new ApplicationException(ex.Message + " " + deleteSql);
				}

				member.CommitChanges();
				return count;				
			}

			public int Count()
			{
				Exception ex = null;
				OdbcConnectionClassX odbcConn = new OdbcConnectionClassX(this.dbConnection);
				string sql = @"select count(*)
								from ICS_CampusGroupUser as cgu
								join ICS_CampusGroup as cg on cg.CampusGroupID = cgu.CampusGroupID
								where cg.Name='@@groupname'
								group by cg.CampusGroupID";
                sql = sql.Replace("@@groupname",this.groupNameEscaped);

				DataTable dt = odbcConn.ConnectToERP(sql,ref ex);
				if (ex != null) throw new ApplicationException(ex.Message + " " + sql);

				return (int) dt.Rows[0].ItemArray[0];
			}
		}

		// This class duplicates CUS.OdbcConnectionClass2, except that the reference to HttpContext is
		// removed so the class can be used with a console application.  We would have based this class
		// on OdbcConnectionClass2 and overridden the GetERPConnectionString()
		// method, but the method is specified "private" and not able to be overridden;
		public class OdbcConnectionClassX
		{
			// Fields
			private string connectionString;

			// Methods
			public OdbcConnectionClassX()
			{
			}

			public OdbcConnectionClassX(string connString)
			{
				if (connString.EndsWith(".xml") || connString.EndsWith(".config"))
				{
					this.connectionString = this.GetERPConnectionString(connString);
				}
				else
				{
					this.connectionString = connString;
				}
			}

			public void ConnectionTest()
			{
				OdbcConnection connection = new OdbcConnection(this.connectionString);
				connection.Open();
				connection.Close();
			}

			public DataTable ConnectToERP(string sqlString)
			{
				Exception ex = null;
				return this.ConnectToERP(sqlString, ref ex);
			}

			public DataTable ConnectToERP(string sqlString, ref Exception ex)
			{
				Exception exReturn = null;
				OdbcConnection myOdbcConnection = new OdbcConnection(this.connectionString);
				DataTable table = null;
				table = this.OdbcConn(myOdbcConnection, sqlString, ref exReturn);
				if (exReturn != null)
				{
					ex = exReturn;
					return table;
				}
				ex = null;
				return table;
			}

			private string GetERPConnectionString(string configFile)
			{
				string str = "";
				string path = configFile;
				if (path.IndexOf("ClientConfig/") < 0)
				{
					path = "ClientConfig/" + path;
				}
				// This private method could not be overridden, so we replaced the following line
				// XmlTextReader reader = new XmlTextReader(HttpContext.Current.Server.MapPath(path));
				XmlTextReader reader = new XmlTextReader(path);
				while (reader.Read())
				{
					if (reader.NodeType == XmlNodeType.Text)
					{
						str = reader.Value.ToString();
					}
				}
				reader.Close();
				return str;
			}

			private DataTable OdbcConn(OdbcConnection myOdbcConnection, string storedProcedure)
			{
				Exception exReturn = null;
				return this.OdbcConn(myOdbcConnection, storedProcedure, ref exReturn);
			}

			private DataTable OdbcConn(OdbcConnection myOdbcConnection, string storedProcedure, ref Exception exReturn)
			{
				myOdbcConnection.Open();
				OdbcCommand command = myOdbcConnection.CreateCommand();
				command.CommandText = storedProcedure;
				OdbcDataAdapter adapter = new OdbcDataAdapter();
				adapter.SelectCommand = command;
				DataSet dataSet = new DataSet();
				string srcTable = "Table Name";
				try
				{
					adapter.Fill(dataSet, srcTable);
				}
				catch (Exception exception)
				{
					exReturn = exception;
					return null;
				}
				DataTable table = dataSet.Tables[srcTable];
				myOdbcConnection.Close();
				exReturn = null;
				return table;
			}
		}

	}
}
