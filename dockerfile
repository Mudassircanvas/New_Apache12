FROM apache/airflow:2.5.0-python3.7
USER root
RUN apt update
RUN apt install -y build-essential

ADD ta-lib/ ./ta-lib
WORKDIR "/opt/airflow/ta-lib"
RUN ./configure --prefix=/usr
RUN make
RUN sudo make install

USER airflow
RUN pip install TA-Lib
RUN pip install pandarallel


WORKDIR "/opt/airflow"
ADD /dags/ETL.py .
ADD /dags/Extract.py .
ADD /dags/Transform.py .
ADD /dags/mlfinlab/requirements.txt .
ADD /dags/mlfinlab/setup.py .

RUN pip install -U requests
RUN pip install -U nest_asyncio

RUN pip install -r requirements.txt
RUN pip install -U llvmlite==0.32.1
RUN pip install cvxpy
RUN python setup.py install --user
ADD /dags/mlfinlab/ .

RUN pip install --upgrade numpy
RUN pip install -U exchange-calendars

CMD ['python', 'dags/ETL.py'] 
