POSTGRES_URL = "142.93.163.88:6006"
POSTGRES_USER = "team15"
POSTGRES_PW = "passw1o5rd"
POSTGRES_DB = "db15"

DB_URL = 'postgresql+psycopg2://{user}:{pw}@{url}/{db}'.format(user=POSTGRES_USER,
                                                               pw=POSTGRES_PW,
                                                               url=POSTGRES_URL,
                                                               db=POSTGRES_DB)