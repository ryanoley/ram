import os
import re
import time
import numpy as np
from subprocess import Popen, PIPE
from ipyparallel import Client
from ram.aws.stream_reader import NonBlockingStreamReader as NBSR

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
        # iterable = strategy.get_terable()
        # wrapper = strategy.get_wrapper()

    def verify_sc_setup(self):
        # verify base config exists
        assert os.path.exists(self.aws_config_base)

        # verify starcluster installed on host machine
        _ = self._star_cmd("starcluster --help")

        # verify aws connection with base config
        self.get_live_clusters()
        return True

    def set_config(self, n_cores, region=None, inst_type='m3.medium'):
        '''
        Create a config for a particular cluster instance.  Fill in fields
        for instance_type, region, n_nodes, and spot bid.  Write the
        config out to /data/ram/aws/configs.
        '''
        n_nodes = n_cores
        # n_nodes = n_cores / 8 for use with c3.8xlarge inst_type
        self._n_nodes = n_nodes

        regionbid = self.get_region_bid(inst_type, region)
        if regionbid is None:
            return
        else:
            self._region = regionbid[0]
            bid = regionbid[1]

        # Read in base config
        with open(self.aws_config_base, 'r') as file:
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
        '''
        Selects an available region based on configured_regions and any
        live clusters. Prompts user for confirmation and returns a spot bid
        '''
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
        '''
        Launch cluster and wait for completion
        '''
        assert os.path.exists(self._config)
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
        '''
        Return information on currently running clusters. Outputs a
        dictionary keyed on cluster name
        '''
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
        ''''
        Return spot price information for instance & region
        '''
        cmd = "starcluster -c {0} -r {1} spothistory -z {2} -c {3}".format(
            self.aws_config_base, region[:-1], region, inst_type)
        stdout = self._star_cmd(cmd)

        pLive = re.findall(r'Current\sprice:\s\$([\.\d]*?)\r\n', stdout)
        pMax = re.findall(r'Max\sprice:\s\$([\.\d]*?)\r\n', stdout)
        pAvg = re.findall(r'Average\sprice:\s\$([\.\d]*?)\r\n', stdout)

        return {'region': region, 'instance': inst_type,
                'live': float(pLive[0]), 'max': float(pMax[0]),
                'avg': float(pAvg[0])}

    def run_parallel(self, function, iterable):
        '''
        Run function across a live cluster.  Iterable must be list-like
        variable whose items can be passed to function.
        '''
        assert hasattr(self, 'client')
        lv = self.client.load_balanced_view()
        results = lv.map(function, iterable)
        # Report progress
        while not results.ready():
            ix = float(results.progress)
            print '{0}% Complete'.format(str(np.round(ix/len(iterable), 2) *
                                             100))
            time.sleep(10)
        return results

    def put_file(self, local_path, remote_path='/ramdata/temp'):
        '''
        Copy file to cluster, default is /ramdata/temp
        '''
        assert os.path.exists(local_path)
        assert self.islive
        cmd = 'starcluster -r {0} -c {1} put {2} {3} {4}'.format(
            self._region[:-1], self._config, self._region, local_path,
            remote_path)
        stdout = self._star_cmd(cmd)
        return

    def get_file(self, local_path, remote_path):
        '''
        Copy file to cluster, will be placed under /ramdata
        '''
        assert os.path.exists(local_path)
        assert self.islive
        cmd = 'starcluster -r {0} -c {1} get {2} {3} {4}'.format(
            self._region[:-1], self._config, self._region, remote_path,
            local_path)
        stdout = self._star_cmd(cmd)
        return

    def teardown(self):
        '''
        Shutdown cluster and cleanup instance variables
        '''
        if self.islive is False:
            print 'No live cluster recognized. Please shutdown explicitly'
            return
        if hasattr(self, 'client'):
            self.client.close()
        self.shutdown_cluster(self._region)
        self._config = None
        self._region = None
        self._ipyconfig = None
        self._n_nodes = None
        return

    def restart_cluster(self):
        '''
        Reboot cluster
        '''
        assert self.islive
        self.client.close()
        cmd = 'starcluster -r {0} -c {1} restart {2}'.format(
            self._region[:-1], self._config, self._region)
        stdout = self._star_cmd(cmd)
        self.client = Client(self._ipyconfig[0], sshkey=self._ipyconfig[1])
        return

    def shutdown_cluster(self, cluster_name):
        '''
        Shutdown a cluster
        '''
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
        '''
        Run a command in subprocess, handle errors and return output from
        stdout
        '''
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = proc.communicate(stdin_txt)
        if proc.poll() != 0:
            print stderr
            raise
        return stdout

    def pull_git_branch(self, repo, branch):
        '''
        Pulls a branch from github in the specified repository. Reboots cluster
        at end of function to ensure cluster is functioning.
        '''
        assert self.islive
        nodes = self.get_live_clusters()[self._region]['ClusterNodes']
        assert len(nodes) == self._n_nodes

        for i in range(self._n_nodes):
            nodei = 'master' if i == 0 else 'node00{0}'.format(i)
            cmd = 'starcluster -r {0} -c {1} sn {2} {3} -u ubuntu'.format(
                self._region[:-1], self._config, self._region, nodei)
            # Open subprocess ssh to cluster node
            proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
            nbsr = NBSR(proc.stdout)
            nbsr.readuntil(timeout=10)
            # Change directory
            proc.stdin.write('cd ~/ramsrc/{} \n'.format(repo))
            nbsr.readuntil(re_error=r'No\ssuch\sfile\sor\sdirectory')
            # /home is NFS shared accross cluster, pull only once on master
            if nodei == 'master':
                # Fetch new changes in repo
                proc.stdin.write('git fetch \n')
                nbsr.readuntil(timeout=7, re_error=r'error')
                # Checkout desired branch
                proc.stdin.write('git checkout {} \n'.format(branch))
                nbsr.readuntil(re_error=r'error')
                #  Pull latest changes
                proc.stdin.write('git pull \n')
                nbsr.readuntil(timeout=7, re_error=r'error')
            # Update python libs on all nodes
            proc.stdin.write('sudo python setup.py install \n')
            nbsr.readuntil(timeout=7,
                           re_error=r'No\ssuch\sfile\sor\sdirectory')
            # Exit ssh session
            proc.stdin.write('exit \n')
            nbsr.readuntil()
            print '/{1}/{2} updated on node: {0}'.format(nodei, repo, branch)
        print 'Rebooting Cluster'
        self.restart_cluster()

if __name__ == '__main__':
    cc = ClusterController('strat')
    cc.set_config(2)
    # cc.launch_cluster()
    cc.teardown()
