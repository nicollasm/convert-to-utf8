import multiprocessing
import os
import time

def processa_linhas(chunkStart, chunkSize, arquivo_origem, arquivo_destino):
    with open(arquivo_origem, 'rb') as f:  # Abre o arquivo no modo binário
        f.seek(chunkStart)
        linhas = f.read(chunkSize).splitlines()
        for i in range(len(linhas)):
            linhas[i] = linhas[i].decode('utf-8', 'ignore').encode('utf-8')  # As linhas já estão em bytes, então esta linha é redundante
        with open(arquivo_destino, 'ab') as dest:  # Abre o arquivo no modo binário
            dest.writelines(linhas)

def chunkify(arquivo_origem, tamanho_chunk=1024*1024):
    tamanho_arquivo = os.path.getsize(arquivo_origem)
    with open(arquivo_origem, 'rb') as f:
        chunkEnd = f.tell()
        while True:
            chunkStart = chunkEnd
            f.seek(tamanho_chunk, 1)
            f.readline()
            chunkEnd = f.tell()
            yield chunkStart, chunkEnd - chunkStart
            if chunkEnd > tamanho_arquivo:
                break

def main(arquivo_origem, arquivo_destino):
    inicio = time.time()
    pool = multiprocessing.Pool()
    jobs = []

    for chunkStart, chunkSize in chunkify(arquivo_origem):
        jobs.append(pool.apply_async(processa_linhas, (chunkStart, chunkSize, arquivo_origem, arquivo_destino)))

    for job in jobs:
        job.get()

    pool.close()
    print(f'Tempo total de processamento: {time.time() - inicio} segundos')

if __name__ == '__main__':
    try:
        caminho_arquivo_origem = 'C:\\Users\\inst\\Documents\\dtusu_dump.sql'
        caminho_arquivo_destino = 'C:\\Users\\inst\\Documents\\dtusu_dump_utf8.sql'
        main(caminho_arquivo_origem, caminho_arquivo_destino)
    except Exception as e:
        print('Ocorreu um erro:', e)
