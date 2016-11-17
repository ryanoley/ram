import os
import re
import time
import numpy as np
import subprocess
from subprocess import Popen, PIPE
from ipyparallel import Client


aws_data = os.path.join(os.getenv('DATA'), 'ram', 'aws')


class ClusterController(object):
    
    configured_regions = ['us-west-1a', 'us-west-2b']
    aws_config_base = os.path.join(os.getenv('GITHUB'), 'ram', 'ram', 'aws',
                                   'config')

    def __init__(self, strategy):
        # check that starcluster is installed and connectivity
        assert self.verify_sc_setup()
        self.islive = False
        # set strategy related vars
        #iterable = strategy.get_terable()
        #wrapper = strategy.get_wrapper()


    def verify_sc_setup(self):
        # verify base config exists
        assert os.path.exists(self.aws_config_base)

        # verify starcluster installed on host machine
        _ = self._star_cmd("starcluster --help")

        # verify aws connection with base config
        self.get_live_clusters()
        return True


    def set_config(self, n_cores, region=None, inst_type='m3.medium'):
        # Set the instance type and n_nodes 
        n_nodes = n_cores

        regionbid = self.get_region_bid(inst_type, region)
        if regionbid is None:
            return
        else:
            self._region = regionbid[0]
            bid = regionbid[1]

        # Read in base config
        with open(self.aws_config_base, 'r') as file :
            configdata = file.read()

        # Configure and save as new
        inst_config = os.path.join(aws_data, 'configs',
                                   'config_{0}'.format(self._region))
        instdata = re.sub(r'CLUSTER_SIZE\s=\s\d+',
                         'CLUSTER_SIZE = {0}'.format(n_nodes),
                         configdata)
        instdata = re.sub(r'NODE_INSTANCE_TYPE.*?\n',
                         'NODE_INSTANCE_TYPE = {0}\n'.format(inst_type),
                         instdata)
        instdata = re.sub(r'SPOT_BID.*?\n',
                         'SPOT_BID = {0}\n'.format(bid),
                         instdata)
        with open(inst_config, 'w') as file:
            file.write(instdata)
        
        self._config = inst_config
        return


    def get_region_bid(self, inst_type, region=None):
        # Available regions
        clusters = self.get_live_clusters()
        inuse = [c['Zone'] for c in clusters.itervalues()]
        avail = list(set(self.configured_regions) - set(inuse))

        if len(avail) == 0:
            print 'All available regions are in use'
            return

        region = region if region is not None else avail[0]
        if region not in avail:
            print 'Unable to provision region {0}'.format(region)
            return

        spot_info = self.get_spot_history(inst_type, region)
        conf = raw_input(('{0} in {1}: live={2}, avg={3}, max={4}. Max bid ' +
                         'will be set at {4}, please confirm (y/n) ').format(
                            inst_type, region, spot_info['live'],
                            spot_info['avg'], spot_info['max']))
        if conf != 'y':
            return

        return (region, spot_info['max'])


    def launch_cluster(self):
        assert os.path.exists(self._config)

        # launch cluster and wait for completion
        cmd = 'starcluster -c {0} -r {1} start -c {2} {2}'.format(
            self._config, self._region[:-1], self._region)
        print 'Launching Cluster'
        stdout = self._star_cmd(cmd)
        print 'Cluster Launched Successfully'
        self.islive = True

        # Setup parallel client 
        ipy_json = re.findall(r"client\s=\sClient\('(.*?)'", stdout)[0]
        ipy_ssh = re.findall(r"sshkey='(.*?)'", stdout)[0]
        self._ipyconfig = (ipy_json, ipy_ssh)
        self.client = Client(ipy_json, sshkey=ipy_ssh)
        return


    def get_live_clusters(self):
        # Return information on currently running clusters.  Return dictionary
        # keyed on cluster name
    
        all_regions = np.unique([x[:-1] for x in self.configured_regions])
        stdoutA = str()
        for region in all_regions:
            cmd = "starcluster -c {0} -r {1} lc".format(self.aws_config_base,
                                                        region)
            stdout = self._star_cmd(cmd)
            stdoutA += '\r\n' + stdout

        # Select cluster info items
        Name = re.findall(r'-+\r\n(.*?)\s\(', stdoutA)
        LaunchTime = re.findall(r'Launch\stime:\s(.*?)\r\n', stdoutA)
        Uptime = re.findall(r'Uptime:\s(.*?)\r\n', stdoutA)
        VPC = re.findall(r'VPC:\s(.*?)\r\n', stdoutA)
        Subnet = re.findall(r'Subnet:\s(.*?)\r\n', stdoutA)
        Zone = re.findall(r'Zone:\s(.*?)\r\n', stdoutA)
        Keypair = re.findall(r'Keypair:\s(.*?)\r\n', stdoutA)
        EbsVolumes = re.findall(r'EBS\svolumes:([\s\S]*?)Spot\srequests',
                                stdoutA)  
        SpotRequests = re.findall(r'Spot\srequests:\s(.*?)\r\n', stdoutA)
        ClusterNodes = re.findall(r'Cluster\snodes:\r\n([\s\S]*?)' +
                                  'Total\snodes', stdoutA)
        TotalNodes = re.findall(r'Total\snodes:\s(.*?)\r\n', stdoutA)

        # Package into dict
        clusters = {}
        for i, name in enumerate(Name):
            clusters[name] = {}
            clusters[name]['LaunchTime'] = LaunchTime[i]
            clusters[name]['Uptime'] = Uptime[i]
            clusters[name]['VPC'] = VPC[i]
            clusters[name]['Subnet'] = Subnet[i]
            clusters[name]['Zone'] = Zone[i]
            clusters[name]['Keypair'] = Keypair[i]
            clusters[name]['EbsVolumes'] = re.findall(r'\s+(.*)?\r\n',
                                                        EbsVolumes[i])
            clusters[name]['SpotRequests'] = SpotRequests[i]
            clusters[name]['ClusterNodes'] = re.findall(r'\s+(.*)?\r\n',
                                                        ClusterNodes[i])
            clusters[name]['TotalNodes'] = TotalNodes[i]

        return clusters


    def get_spot_history(self, inst_type, region):
        # Get spot price information for instance & region

        cmd = "starcluster -c {0} -r {1} spothistory -z {2} -c {3}".format(
            self.aws_config_base, region[:-1], region, inst_type)
        stdout = self._star_cmd(cmd)

        pLive = re.findall(r'Current\sprice:\s\$([\.\d]*?)\r\n', stdout)
        pMax = re.findall(r'Max\sprice:\s\$([\.\d]*?)\r\n', stdout)
        pAvg = re.findall(r'Average\sprice:\s\$([\.\d]*?)\r\n', stdout)
        
        return {'region':region, 'instance':inst_type, 'live':float(pLive[0]),
                'max':float(pMax[0]), 'avg':float(pAvg[0])}


    def run_parallel(self, function, iterable):
        assert hasattr(self, 'client')
        lv = self.client.load_balanced_view()
        results = lv.map(function, iterable)
        # Report progress
        while results.ready() == False:
            ix = float(results.progress)
            print '{0}% Complete'.format(str(np.round(ix/len(iterable), 2) *
                                             100))
            time.sleep(10)
        return results


    def put_file(self, local_path, remote_path='/ramdata/temp'):
        # Copy file to cluster, default is /ramdata/temp
        assert os.path.exists(local_path)
        assert self.islive
        cmd = 'starcluster -r {0} -c {1} put {2} {3} {4}'.format(
            self._region[:-1], self._config, self._region, local_path,
            remote_path)
        stdout = self._star_cmd(cmd)
        return


    def get_file(self, local_path, remote_path):
        # Copy file to cluster, will be placed under /ramdata
        assert os.path.exists(local_path)
        assert self.islive
        cmd = 'starcluster -r {0} -c {1} get {2} {3} {4}'.format(
            self._region[:-1], self._config, self._region, remote_path,
            local_path)
        stdout = self._star_cmd(cmd)
        return


    def teardown(self):
        # Shutdown cluster
        if self.islive is False:
            print 'No live cluster recognized. Please shutdown explicitly'
            return
        if hasattr(self, 'client'):
            self.client.close()
        self.shutdown_cluster(self._region)
        self._config = None
        self._region = None
        self._ipyconfig = None
        return


    def restart_cluster(self):
        # Reboot cluster 
        assert self.islive
        self.client.close()
        cmd = 'starcluster -r {0} -c {1} restart {2}'.format(
            self._region[:-1], self._config, self._region)
        stdout = self._star_cmd(cmd)
        self.client = Client(self._ipyconfig[0], sshkey=self._ipyconfig[1])
        return


    def shutdown_cluster(self, cluster_name):
        # Shutdown a cluster
        cmd = 'starcluster -c {0} -r {1} terminate -f {2}'.format(
            self.aws_config_base, cluster_name[:-1], cluster_name)
        stdout = self._star_cmd(cmd, 'y')
        # Delete config if exists
        config_path = os.path.join(aws_data, 'configs',
                                   'config_{0}'.format(cluster_name))
        if os.path.exists(config_path):
            os.remove(config_path)
        print 'Cluster shutdown complete'
        return


    def _star_cmd(self, cmd, stdin_txt=None):
        # Run a command in subprocess, handle errors and return output from
        # stdout
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = proc.communicate(stdin_txt)
        if proc.poll() != 0:
            print stderr
            raise
        return stdout




# Testing functions
iterable = range(30)
def simple_iter(ix):
    import socket
    import time
    host = socket.gethostname()
    out_name = 'out_{0}${1}'.format(host, str(ix))
    # pause to monitor progress
    time.sleep(3)
    return out_name

iterable = range(5)
def db_connect(dt):
    import pypyodbc
    connection = pypyodbc.connect('DSN=mssql;uid=ramuser;pwd=183madison')
    cursor = connection.cursor()
    status = connection.connected
    connection.close()
    return status

import pandas as pd
datelist = [x.date() for x in pd.date_range(pd.datetime.today(), periods=30)]
def db_select(dt):
    import datetime
    import pypyodbc
    import time
    import pandas as pd
    connection = pypyodbc.connect('DSN=mssql;uid=ramuser;pwd=183madison')
    cursor = connection.cursor()
    sqlcmd = "select * from ram.dbo.trading_dates where CalendarDate = '{}'".format(dt)
    cursor.execute(sqlcmd)
    out = pd.DataFrame(data=cursor.fetchall(), columns=['CalendarDate','Weekday','T0',
                                                        'Tmd','Tm2','Tm1','T1','T2','T3'])
    out.index = out.CalendarDate.values
    connection.close()
    time.sleep(3)
    return out


def db_write(dt):
    import datetime
    import pypyodbc
    import pandas as pd
    connection = pypyodbc.connect('DSN=mssql;uid=ramuser;pwd=183madison')
    cursor = connection.cursor()
    sqlcmd = "select * from ram.dbo.trading_dates where CalendarDate = '{}'".format(dt)
    cursor.execute(sqlcmd)
    out = pd.DataFrame(data=cursor.fetchall(),
                       columns=['CalendarDate','Weekday','T0','Tmd','Tm2',
                                'Tm1','T1','T2','T3'])
    connection.close()
    import socket
    host = socket.gethostname()
    out_name = '/ramdata/temp/out_{0}${1}.csv'.format(host, str(dt))
    out.to_csv(out_name)
    return host





if __name__ == '__main__':
    cc = ClusterController('strat')
    cc.set_config(2)
    #cc.launch_cluster()
    cc.teardown()




