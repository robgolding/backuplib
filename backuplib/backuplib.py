import os, sys, random, string, shutil, subprocess

class BackupError(Exception):
	pass

class ServerError(Exception):
	pass

class Backup:
	
	class Job:
		def __init__(self, backup, path, name=None):
			self.backup = backup
			self.path = path
			self.name = name or os.path.basename(os.path.abspath(self.path))
		
		def execute(self):
			dst = os.path.join(self.backup.temp_dir, self.name)
			self.backup._rsync(self, dst)
		
		def __str__(self):
			return self.path
	
	def __init__(self, name, dst, gzip=True, debug=False):
		self.name = name
		self.dst = dst
		self.gzip = gzip
		self.debug = debug
		self.temp_dir = os.path.join("/tmp/", 'rsync-'+''.join([random.choice(string.digits+string.letters) for i in range(7)]))
		
		self.jobs = []
		
		self.executed = False
	
	def _system(self, command):
		if self.debug: print command
		try:
			retcode = subprocess.call([command], shell=True)
			if retcode != 0:
				print >>sys.stderr, "Child exited with signal", retcode
				raise BackupError, "Error executing command: %s" % command
		except OSError, e:
			print >>sys.stderr, "Execution failed:", e
	
	def _rsync(self, job, dst):
		command = "rsync -aA '%s' '%s'" % (job, dst)
		self._system(command)
	
	def _cleanup(self):
		command = "rm -rf '%s'" % self.temp_dir
		self._system(command)
	
	def add_job(self, path, name=None):
		self.jobs.append(self.Job(self, path, name))
	
	def get_size(self):
		if not self.executed: return None
		command = "ls -lh '%s' | cut -d ' ' -f 5" % self.dst
		p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		stdout, stderr = p.communicate()
		return stdout.strip()
	
	def execute(self):
		print 'Executing job: %s' % self.dst
		os.mkdir(self.temp_dir)
		for job in self.jobs:
			job.execute()
		
		tar_opts = "-zcf" if self.gzip else "-cf"
		
		command = "cd '%s' && tar %s '%s' *" % (self.temp_dir, tar_opts, self.dst)
		self._system(command)
		
		self._cleanup()
		
		self.executed = True

class RemoteBackup(Backup):
	
	class Server:
		def __init__(self, hostname, user, keyfile=None):
			self.hostname = hostname
			self.user = user
			self.keyfile = os.path.abspath(keyfile)

	class Job(Backup.Job):
		def __init__(self, backup, server, path, name=None):
			self.backup = backup
			self.server = server
			self.path = path
			self.name = name or os.path.basename(os.path.abspath(self.path))
		
		def get_remote_shell(self):
			if self.server.keyfile:
				return "ssh -qq -o 'StrictHostKeyChecking no' -o 'UserKnownHostsFile=/dev/null' -i '%s'" % self.server.keyfile
			else:
				return "ssh -qq -o 'StrictHostKeyChecking no' -o 'UserKnownHostsFile=/dev/null'"
		
		def __str__(self):
			return "%s@%s:%s" % (self.server.user, self.server.hostname, self.path)
	
	def __init__(self, name, dst, gzip=True, debug=False):
		self.name = name
		self.dst = dst
		self.gzip = gzip
		self.debug = debug
		self.temp_dir = os.path.join("/tmp/", 'rsync-'+''.join([random.choice(string.digits+string.letters) for i in range(7)]))
		
		self.servers = {}
		self.jobs = []
	
	def _rsync(self, job, dst):
		rsh = '-e "%s"' % job.get_remote_shell()
		command = "rsync -aA %s '%s' '%s'" % (rsh, job, dst)
		self._system(command)
	
	def add_server(self, hostname, user, keyfile=None):
		self.servers[hostname] = self.Server(hostname, user, keyfile)
	
	def add_job(self, server, remote_path, name=None):
		if isinstance(server, basestring):
			server_obj = self.servers.get(server, None)
			if not server_obj:
				raise ServerError, "Cannot find server '%s'" % server
		else:
			server_obj = server
		self.jobs.append(self.Job(self, server_obj, remote_path, name))

class SnapshotBackup:
	
	def __init__(self, name, src, dst, snapshots=3, logfile=None, exclude_list=[], modify_window=3, debug=False):
		self.name = name
		self.src = src
		self.dst = dst
		self.snapshots = snapshots
		self.logfile = logfile
		self.exclude_list = exclude_list
		self.modify_window = modify_window
		self.debug = debug
		
		self.executed = False
		
		self.final_dst = os.path.join(self.dst, "%s.%d" % (self.name, 1))
	
	def _system(self, command):
		if self.debug: print command
		try:
			retcode = subprocess.call([command], shell=True)
			if retcode != 0:
				print >>sys.stderr, "Child exited with signal", retcode
				raise BackupError, "Error executing command: %s" % command
		except OSError, e:
			print >>sys.stderr, "Execution failed:", e
	
	def get_size(self):
		if not self.executed: return None
		command = "du -sh '%s' | cut -f1" % self.final_dst
		p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		stdout, stderr = p.communicate()
		return stdout.strip()
	
	def execute(self):
		print 'Executing job: %s' % self.name
		for i in range(self.snapshots, 0, -1):
			this_snapshot = os.path.normpath("%s.%d" % (self.name, i))
			next_snapshot = os.path.normpath("%s.%d" % (self.name, i+1))
			this_path = os.path.join(self.dst, this_snapshot)
			next_path = os.path.join(self.dst, next_snapshot)
			if not os.path.isdir(this_path):
				if self.debug: print "Creating %s" % this_path
				os.mkdir(this_path)
			if self.debug: print "Moving %s to %s" % (this_path, next_path)
			shutil.move(this_path, next_path)

		link_dest = next_path
		destination = this_path

		remove_path = os.path.join(self.dst, os.path.normpath("%s.%d" % (self.name, self.snapshots+1)))
		shutil.rmtree(remove_path)
		
		exclude = ''
		for e in self.exclude_list:
			exclude += " --exclude '%s'" % e
			
		verbosity = 'vv' if self.debug else 'v'
		
		command = "rsync -aA%s --link-dest='%s' %s --modify-window=%d '%s' '%s'" % (verbosity, link_dest, exclude, self.modify_window, self.src, destination)

		if self.logfile:
			command = "%s | tee '%s'" % (command, self.logfile)
		
		self._system(command)
		
		self.executed = True
