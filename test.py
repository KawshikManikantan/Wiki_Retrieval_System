import resource
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
print(soft,hard)