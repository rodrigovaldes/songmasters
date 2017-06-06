
# File with implementations to create .sh files to send and receive information
# to the nodes

# List internal IDs
mozart_int = "XXX.XXX.XXX.XX"
internal_ids = ["XX.XXX.X.7",
"XX.XXX.X.13",
"XX.XXX.X.19",
"XX.XXX.X.20",
"XX.XXX.X.12",
"XX.XXX.X.3",
"XX.XXX.X.18",
"XX.XXX.X.16",
"XX.XXX.X.10",
"XX.XXX.X.23",
"XX.XXX.X.21",
"XX.XXX.X.22",
"XX.XXX.X.15",
"XX.XXX.X.14",
"XX.XXX.X.8",
"XX.XXX.X.24",
"XX.XXX.X.11",
"XX.XXX.X.17",
"XX.XXX.X.9",
"XX.XXX.X.6"]

# List external IDs
mozart_ext = "XXX.XXX.XXX.XX"
external_ids = ["XX.XXX.X.7",
"XX.XXX.X.13",
"XX.XXX.X.19",
"XX.XXX.X.20",
"XX.XXX.X.12",
"XX.XXX.X.3",
"XX.XXX.X.18",
"XX.XXX.X.16",
"XX.XXX.X.10",
"XX.XXX.X.23",
"XX.XXX.X.21",
"XX.XXX.X.22",
"XX.XXX.X.15",
"XX.XXX.X.14",
"XX.XXX.X.8",
"XX.XXX.X.24",
"XX.XXX.X.11",
"XX.XXX.X.17",
"XX.XXX.X.9",
"XX.XXX.X.6"]

######## SEND SLIDERING #########
bin_bash = "#!/bin/sh"
prefix = "scp -i ~/.ssh/insert-key slither.sh user@"
suffix = ":~/  # Beethoven{}"

scp_list = []
scp_list.append(bin_bash)
for j, ip in enumerate(external_ids):
    new_thing = prefix + str(ip) + str(suffix.format(j))
    scp_list.append(new_thing)

thefile = open('send_slidering.sh', 'w')
for item in scp_list:
    thefile.write("%s\n" % item)
thefile.close()

######## SEND PICKLE PAIRS #########
pfix = "scp -i ~/.ssh/insert-key ../mpi/pp_new-emo.py user@"

scp_pp = []
scp_pp.append(bin_bash)
for j, ip in enumerate(external_ids):
    command = pfix + str(ip) + str(suffix.format(j))
    scp_pp.append(command)

file = open('send_ppairs.sh', 'w')
for item in scp_pp:
    file.write("%s\n" % item)
file.close()

######## SEND KEYS #########
initial = "scp -i ~/.ssh/insert-key insert-key user@"

scp_kk = []
scp_kk.append(bin_bash)
for j, ip in enumerate(external_ids):
    i_command = initial + str(ip) + str(suffix.format(j))
    scp_kk.append(i_command)

thefile = open('send_keys.sh', 'w')
for item in scp_kk:
    thefile.write("%s\n" % item)
thefile.close()

######## host_file ############
h = open('hosts', 'w')
for item in internal_ids:
    h.write("%s\n" % item)
h.close()

zero = "scp -i ~/.ssh/insert-key hosts user@"

hosts_s = []
hosts_s.append(bin_bash)
for j, ip in enumerate(external_ids):
    executable = zero + str(ip) + str(suffix.format(j))
    hosts_s.append(executable)

thefile = open('send_host.sh', 'w')
for item in hosts_s:
    thefile.write("%s\n" % item)
thefile.close()

####### SSH Things ########
zero = "ssh -i ~/.ssh/insert-key user@"
hosts_s = []
hosts_s.append(bin_bash)
for j, ip in enumerate(external_ids):
    executable = zero + str(ip)
    hosts_s.append(executable)

thefile = open('ssh_s.sh', 'w')
for item in hosts_s:
    thefile.write("%s\n" % item)
thefile.close()

####### SSH Things ########
ssh_internal_ids = []
for internal in internal_ids:
    login = "ssh " + str(internal)
    ssh_internal_ids.append(login)

thefile = open('ssh_mozart_to_everyone.sh', 'w')
for item in ssh_internal_ids:
    thefile.write("%s\n" % item)
thefile.close()

####### SSH Things ########
scp_testing = []
scp_testing.append(bin_bash)
for num, ip in enumerate(external_ids):
    login = "scp -i ~/.ssh/insert-key ../auxiliary_code/testing_mpi.py user@" + str(ip) + ":~/  # Beethoven{}".format(num)
    scp_testing.append(login)

thefile = open('send_testing.sh', 'w')
for item in scp_testing:
    thefile.write("%s\n" % item)
thefile.close()


# Sent to Mozart
# scp -i ~/.ssh/insert-key hosts user@XXX.XXX.XXX.XX:
# scp -i ~/.ssh/insert-key ../auxiliary_code/testing_mpi.py user@XXX.XXX.XXX.XX:
# scp -i ~/.ssh/insert-key ~/.ssh/insert-key user@XXX.XXX.XXX.XX:~/  # Mozart
# scp -i ~/.ssh/insert-key ../mpi/pp_new-emo.py user@XXX.XXX.XXX.XX:~/  # Mozart
# scp -i ~/.ssh/insert-key slither.sh user@XXX.XXX.XXX.XX:~/  # Mozart
# scp -i ~/.ssh/insert-key hosts user@XXX.XXX.XXX.XX:~/  # Mozart
# scp -i ~/.ssh/insert-key ../mpi/pickler.py user@XXX.XXX.XXX.XX:~/  # Mozart
# scp -i ~/.ssh/insert-key ../mpi/build_fname_list.py user@XXX.XXX.XXX.XX:~/  # Mozart





