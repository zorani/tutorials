# Copyright 2015 Zoran Ilievski , clientuser.net
#
# This code was created for a ClientUser.net tutorial on how to write StarCluster plugins.
# Which can be viewed at the following location https://youtu.be/0W-l42oeu98
#
# This file is a fully functioning MySQL-Cluster plugin for StarCluster and instructions 
# on use are as follows
#
#  1] Place this file "simplemysql.py" to your /home/user/.starcluster/plugin directory
#
#  2] Edit the /home/user/.starcluster/config file and make the following additions.
#
#     State you want the simplemysqlcluster plugin in your cluster template
#
#
# Dont forget to uncomment the config lines!

################  /home/user/.starcluster/config #####################################
################################
## Defining Cluster Templates ##
################################

#[cluster smallcluster]
#PLUGINS =  simplemysqlcluster

#####################################
## Configuring StarCluster Plugins ##
#####################################

#[plugin simplemysqlcluster]
#SETUP_CLASS = starcluster.plugins.simplemysql.SimpleMysqlCluster
#NUM_REPLICAS = 1
#NUM_DATA_NODES = 1
#NUM_SQL_NODES = 1
#SQL_USER = someuser
#SQL_PASSWORD = somepassword
####################################################################################

# 3]  You can also compile this as a "built in" plugin, more details at https://youtu.be/0W-l42oeu98

from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log

class SimpleMysqlCluster(DefaultClusterSetup):


    def __init__(self, num_replicas, num_sql_nodes,  num_data_nodes, sql_user, sql_password):
        super(SimpleMysqlCluster, self).__init__()
        self._num_replicas = int(num_replicas)
        self._num_sql_nodes = int(num_sql_nodes)
        self._num_data_nodes = int(num_data_nodes)
        self._sql_user = sql_user
        self._sql_password = sql_password


    def run(self, nodes, master, user, user_shell, volumes):
        log.info("plugin SimpleMysqlCluster: Installing mysql-cluster-server on all nodes...")
        self._InitialChecks(nodes)
        self._DefineNodeLists(nodes, master)
        #Following lists now exist.
        #self.data_nodes_list, self.sql_nodes_list, self.data_alias_list, self.sql_alias_list
        #For all nodes
        for node in nodes:
            self.pool.simple_job(self._PurgeMySQL, (node), jobid=node.alias)
        self.pool.wait(len(nodes))
        #For data and sql nodes
        for node in (self.data_nodes_list + self.sql_nodes_list):
            self.pool.simple_job(self._InstallLibs,(node), jobid=node.alias)
            self.pool.simple_job(self._MysqlUserGroup,(node), jobid=node.alias)
        self.pool.wait(len(self.data_nodes_list + self.sql_nodes_list))
        for node in nodes:
            self.pool.simple_job(self._GetMysqlClusterPackage, (node), jobid=node.alias)
        self.pool.wait(len(nodes))
        #For management node
        self._CopyManagementNodeScripts(master)
        #For data
        for node in (self.data_nodes_list):
            self.pool.simple_job(self._CopyDataNodeScripts, (node), jobid=node.alias)
        self.pool.wait(len(self.data_nodes_list))
        #For sql
        for node in (self.sql_nodes_list):
            self.pool.simple_job(self._CopySqlNodeScripts, (node), jobid=node.alias)
        self.pool.wait(len(self.sql_nodes_list))
        #configure master node
        self._ManagementNodeConfigSetup(master)
        #configure data nodes, then start them all
        for node in (self.data_nodes_list):
            self.pool.simple_job(self._DataNodeConfigSetup, (node), jobid=node.alias)
        self.pool.wait(len(self.data_nodes_list))
        #configure sql nodes, then start them all
        for node in (self.sql_nodes_list):
            self.pool.simple_job(self._SqlNodeConfigSetup, (node), jobid=node.alias)
            self.pool.simple_job(self._UpdateAppArmor, (node), jobid=node.alias)
            self.pool.simple_job(self._SqlNodeDataBaseInstall, (node), jobid=node.alias)
        self.pool.wait(len(self.sql_nodes_list))
        log.info("plugin SimpleMysqlCluster: Starting the mysql cluster")
        #Start up the cluster by starting up the nodes in the correct order.
        for node in ([master]):
            log.info("plugin SimpleMysqlCluster: Starting the mysql master node")
            self.pool.simple_job(self._StartManagementNode, (node), jobid=node.alias)
        self.pool.wait(len([master]))
        log.info("plugin SimpleMysqlCluster: Starting the mysql data nodes")
        for node in (self.data_nodes_list):
            self.pool.simple_job(self._StartDataNode, (node),  jobid=node.alias)
        self.pool.wait(len(self.data_nodes_list))
        log.info("plugin SimpleMysqlCluster: Starting the mysql sql nodes")
        for node in (self.sql_nodes_list):
            self.pool.simple_job(self._StartSqlNode, (node),  jobid=node.alias)
        self.pool.wait(len(self.sql_nodes_list))
        #You only really need to connect to one SQL node to execute the add user to mysql.
        #I just pick the first SQL node in the sql nodes list.
        for node in (self.sql_nodes_list):
            self.pool.simple_job(self._AddRemoteUsersToSQLNodes, (node),  jobid=node.alias)
        self.pool.wait(len(self.sql_nodes_list))

    def _InitialChecks(self,nodes):
        log.info("plugin SimpleMysqlCluster: Carrying out initial configuration checks...")
        #Test 1: Sum of requested sql-cluster nodes must equal total number of StarCluster nodes.
        NumberOfStarClusterNodes=len(nodes)
        NumberOfMySqlClusterNodes=1 + self._num_sql_nodes + self._num_data_nodes
        if NumberOfStarClusterNodes == NumberOfMySqlClusterNodes:
            log.info("plugin SimpleMysqlCluster: TEST PASS - Does number of requested sql nodes match number of requested StarCluster nodes?")
        else:
            log.info("plugin SimpleMysqlCluster: TEST FAIL - Does number of requested sql nodes match number of requested StarCluster nodes?")
            raise RuntimeError('ERROR simplemysqlcluster plugin:Number of starcluster nodes is not equal \n to the number of mysqlcluster nodes you have requested. \n please correct this')
        #Test 2: Number of replicas must be set to either 1 or 2.
        if ((self._num_replicas == 1) or (self._num_replicas == 2)):
            log.info("plugin SimpleMysqlCluster: TEST PASS - NUM_REPLICAS set to either 1 or 2")
        else:
            log.info("plugin SimpleMysqlCluster: TEST FAIL - NUM_REPLICAS not set correctly to either 1 or 2")
            log.info("plugin SimpleMysqlCluster: TEST FAIL - Setting NUM REPLICAS to 2")
            self._num_replicas=2
        #Test 3: If Number of replicas is 2, the number of data nodes must be divisible by 2.
        if (self._num_replicas == 2):
            if (self._num_data_nodes%2 == 0 ): 
                log.info("plugin SimpleMysqlCluster: TEST PASS - Number data nodes divisable by 0, number of replicas set to 2.")
            else:
                log.info("plugin SimpleMysqlCluster: TEST FAIL - Number of data nodes not divisable by 0, even though replicas set to 2")
                log.info("plugin SimpleMysqlCluster: TEST FAIL - Setting number or replicas to 1 ...")
                self._num_replicas=1
        else:
            log.info("plugin SimpleMysqlCluster: TEST PASS - Number of replicas set to 1, compatable with even and odd number of data nodes")
        #More information on mysql-cluster structure http://dev.mysql.com/doc/refman/5.0/en/mysql-cluster-nodes-groups.html
        log.info("plugin SimpleMysqlCluster: SimpleMySqlCluster Plugin Initial Checks Completed.")


    def _DefineNodeLists(self, nodes, master):
        log.info("plugin SimpleMysqlCluster:  Generating Node Lists...")
        #Partition node objects in to data and query nodes lists.
        self.data_nodes_list = nodes[1:self._num_data_nodes + 1]
        self.sql_nodes_list = nodes[self._num_data_nodes + 1:]
        #Extract StarCluster aliases for each type of node
        self.data_alias_list = [x.alias for x in self.data_nodes_list]
        self.sql_alias_list = [x.alias for x in self.sql_nodes_list]


    def _GetMysqlClusterPackage(self, node):
        #
        # ALL NODES: get mysql cluster tar.gz package
        #
        commands='''
                 touch hello.txt;
                 cd /var/tmp/;
                 wget http://dev.mysql.com/get/Downloads/MySQL-Cluster-7.3/mysql-cluster-gpl-7.3.7-linux-glibc2.5-x86_64.tar.gz;
                 tar -xvzf mysql-cluster-gpl-7.3.7-linux-glibc2.5-x86_64.tar.gz
                 '''
        nconn = node.ssh
        nconn.execute(commands)
        log.info("plugin SimpleMysqlCluster:  Wgetting mysqlcluster package...")


    def _PurgeMySQL(self, node):
        #
        # ALL NODES: UNINSTALL EXISTING MYSQL
        #
        commands='''
                 export DEBIAN_FRONTEND=noninteractive
                 rm -rf /var/lib/mysql
                 rm -rf /etc/mysql/
                 rm -f /etc/mysql/my.cnf
                 apt-get -y remove mysql*
                 apt-get -y --purge remove
                 apt-get -y autoremove
                 dpkg --get-selections | grep mysql
                 aptitude -y purge $(dpkg --get-selections | grep deinstall | sed s/deinstall//)
                 '''
        nconn = node.ssh
        nconn.execute(commands)


    def _InstallLibs(self, node):
        #
        # DATA,SQL NODES: install libaio1 libaio=dev
        #
        commands='''
                 sudo apt-get -y install libaio1 libaio-dev
                 '''
        nconn = node.ssh
        nconn.execute(commands)


    def _MysqlUserGroup(self, node):
        #
        # DATA, SQL NODES: create user, group
        #
        commands='''
                 groupadd mysql || true
                 useradd -g mysql mysql || true
                 '''
        nconn = node.ssh
        nconn.execute(commands)


    def _CopyManagementNodeScripts(self, node):
        #
        # MANAGEMENT NODE: only ndb_mgm* scripts are needed.
        #
        commands='''
                 cp /var/tmp/mysql-cluster-gpl-7.3.7-linux-glibc2.5-x86_64/bin/ndb_mgm* /usr/local/bin/
                 rm -rf /var/tmp/*
                 '''
        nconn = node.ssh
        nconn.execute(commands)


    def _CopyDataNodeScripts(self, node):
        #
        # DATA NODE: only ndbd and ndbmtd are needed from the package.
        #
        commands='''
                 cp /var/tmp/mysql-cluster-gpl-7.3.7-linux-glibc2.5-x86_64/bin/ndbd /usr/local/bin/ndbd
                 cp /var/tmp/mysql-cluster-gpl-7.3.7-linux-glibc2.5-x86_64/bin/ndbmtd /usr/local/bin/ndbmtd
                 rm -rf /var/tmp/*
                 '''
        nconn = node.ssh
        nconn.execute(commands)


    def _CopySqlNodeScripts(self, node):
        #
        # SQL NODE: copy cluster directory to /usr/local/ create soft link
        #
        commands='''
                 cp -r /var/tmp/mysql-cluster-gpl-7.3.7-linux-glibc2.5-x86_64/ /usr/local/
                 ln -s /usr/local/mysql-cluster-gpl-7.3.7-linux-glibc2.5-x86_64/ /usr/local/mysql-cluster
                 rm -rf /var/tmp/*
                 '''
        nconn = node.ssh
        nconn.execute(commands)


    def _ManagementNodeConfigSetup(self, node):
        #
        # MANAGEMENT NODE: create custom config and data dir.
        #
        ndbd_default='''
[ndbd default]
NoOfReplicas=%(num_replicas)s
DataMemory=80M
IndexMemory=18M
                      ''' % {'num_replicas': self._num_replicas}
        ndb_mgmd='''
[ndb_mgmd]
hostname=master
datadir=/var/lib/mysql-cluster
                 '''
        ndbd_data_node_part='''
[ndbd]
hostname=%(alias)s
datadir=/usr/local/mysql/data
                            '''
        ndbd_data_node_full=''
        for x in (self.data_alias_list):
            ndbd_data_node_full = ndbd_data_node_full + '\n' + ndbd_data_node_part % {'alias': x}
        mysqld_sql_node_part='''
[mysqld]
hostname=%(alias)s
                             '''
        mysqld_sql_node_full=''
        for x in (self.sql_alias_list):
            mysqld_sql_node_full = mysqld_sql_node_full + '\n' + mysqld_sql_node_part % {'alias': x}
        commands='''
mkdir -p /var/lib/mysql-cluster/
cat > /var/lib/mysql-cluster/config.ini << EOF
#####################################
#  Config File For Management Node  #
#####################################
#
#  Which node?  
#  Answer: Master.
#
#  Which location?
#  Answer: /var/lib/mysql-cluster/config.ini
%(ndbd_default)s  
%(ndb_mgmd)s
%(ndbd)s   
%(mysqld)s
EOF
''' % {'ndbd_default': ndbd_default,
                        'ndb_mgmd': ndb_mgmd,
                        'ndbd': ndbd_data_node_full,
                        'mysqld': mysqld_sql_node_full}
        nconn = node.ssh
        nconn.execute(commands)


    def _StartManagementNode(self, node):
        #
        # MANAGEMENT NODE: Startup Command
        #
        commands='ndb_mgmd --configdir=/var/lib/mysql-cluster/ -f /var/lib/mysql-cluster/config.ini'
        nconn = node.ssh
        nconn.execute(commands)


    def _DataNodeConfigSetup(self, node):
        #
        # DATA NODE: create data dir, write config.
        #
        commands='''
mkdir -p /usr/local/mysql/data
rm -f /etc/mysql/my.cnf
rm -f /usr/local/mysql/my.cnf
cat > /etc/my.cnf << EOF
#####################################
# Config for  SQL Node & Data Nodes #
#####################################
#
#
#  Place in following location:
#
#  /etc/my.cnf
#
#



[mysqld]
ndbcluster 


[mysql_cluster]
ndb-connectstring=master

EOF
'''
        nconn = node.ssh
        nconn.execute(commands)


    def _StartDataNode(self, node):
        #
        # DATA NODE: Startup Command
        #
        commands='ndbd'
        nconn = node.ssh
        nconn.execute(commands)


    def _SqlNodeConfigSetup(self, node):
        #
        # SQL NODE: write config file.
        #
        commands='''
cat > /etc/my.cnf << EOF
#####################################
# Config for  SQL Node & Data Nodes #
#####################################
#
#
#  Place in following location:
#
#  /etc/my.cnf
#
#



[mysqld]
ndbcluster


[mysql_cluster]
ndb-connectstring=master

[mysqld]
basedir=/usr/local/mysql-cluster/

EOF
'''
        nconn = node.ssh
        nconn.execute(commands)


    def _UpdateAppArmor(self, node):
        #
        # SQL NODE: update and restart AppArmor
        #
        #NOTE: About non-default installs.
        #Moving database away from default location /var/lib/mysql will generate errors when
        #running mysql_install_db, until you,
        #update the usr.sbin.mysql file in /etc/apparmor.d/ , then run 
        #/etc/init.d/apparmor restart
        commands='''
                 cat > /etc/apparmor.d/usr.sbin.mysqld << EOF

                 /usr/local/mysql-cluster/data/ r,
                 /usr/local/mysql-cluster/data/** rwk,


                 EOF
                 service apparmor reload
                 '''
        nconn = node.ssh
        nconn.execute(commands)


    def _SqlNodeDataBaseInstall(self, node):
        #
        # SQL NODE: Install the database
        #
        commands='''
                 /usr/local/mysql-cluster/scripts/mysql_install_db --user=mysql --basedir=/usr/local/mysql-cluster/ --datadir=/usr/local/mysql-cluster/data/ --defaults-file=/etc/my.cnf 
                 cp /usr/local/mysql-cluster/bin/mysqld_safe /usr/bin/
                 cp /usr/local/mysql-cluster/bin/mysql /usr/bin/
                 cd /usr/local/mysql-cluster
                 chown -R root .
                 chown -R mysql data
                 chgrp -R mysql .
                 cp /usr/local/mysql-cluster/support-files/mysql.server /etc/init.d/
                 #need to set the following variables in file /etc/init.d/mysql.server to  
                 basedir=/usr/local/mysql-cluster/
                 datadir=/usr/local/mysql-cluster/data/
                 sed -i 's/^basedir=$/basedir=\/usr\/local\/mysql-cluster\//g' /etc/init.d/mysql.server
                 sed -i 's/^datadir=$/datadir=\/usr\/local\/mysql-cluster\/data\//g' /etc/init.d/mysql.server
                 cd /etc/init.d/
                 update-rc.d mysql.server defaults
                 '''
        nconn = node.ssh
        nconn.execute(commands)


    def _StartSqlNode(self, node):
        #
        # SQL NODE: Startup command.
        #
        commands='''
                 service mysql.server start
                 '''
        nconn = node.ssh
        nconn.execute(commands)


    def _AddRemoteUsersToSQLNodes(self, node):
        #
        # SQL NODE: Create user on sql database, with privileges and access from anywhere
        #
        commands='''
mysql -e "CREATE USER \'%(someuser)s\'@\'%%\' IDENTIFIED BY \'%(somepassword)s\'";
mysql -e "GRANT ALL PRIVILEGES ON *.* TO \'%(someuser)s\'@\'%%\' WITH GRANT OPTION";
''' % {'someuser': self._sql_user, 'somepassword': self._sql_password}
        nconn = node.ssh
        nconn.execute(commands)
