def listFilesHDFS(path):
    '''
    :param path: diretorio que deseja listar os arquivos
    :return: lista com os arquivos no diretorio informado
    '''
    cmd = ['hdfs', 'dfs', '-lsr', '-C', path]
    files = subprocess.check_output(cmd).strip().split('\n')
    return files
