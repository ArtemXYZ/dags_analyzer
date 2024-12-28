"""
    Анализ имеющихся дагов в Airflow, их параметров, поиск неактуальных.
"""

from datetime import datetime, timedelta
# ---------------------------------- airflow
from airflow.models import DagBag, TaskInstance  # , DagModel, DagRun
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------------------------- Импорт сторонних библиотек
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
# -------------------------------- Локальные модули
import bot_alert_telegram as bt
import task_logger as tl

# from table_commentator import TableCommentator
from DAGs.table_commentator.table_commentator import TableCommentator
# ----------------------------------------------------------------------------------------------------------------------
CONFIG_MART_SV = {
    'drivername': '',
    'username': '',
    'password': '',
    'host': '',
    'port': 5432,
    'database': ''
}
# ----------------------------------------------------------------------------------------------------------------------

class DagsAnalyzer:
    """
        Анализ имеющихся дагов в Airflow, их параметров, поиск неактуальных.

        Классы и методы Airflow:
            Используйте DagBag для текущих загруженных DAG-ов.
            Подключайте DagModel для проверки использования и активности.
            Применяйте DagRun для анализа выполнений.
            # Если нужно, добавляйте TaskInstance для детализации по задачам. - не точно, надо разбираться!

        Как Airflow работает с DAG?

        Файлы DAG в папке:
            Когда вы добавляете DAG в папку dags_folder (например, через GitLab),
            Airflow автоматически парсит эти файлы.

            За парсинг отвечает компонент DagBag. Он читает Python-файлы в dags_folder и загружает DAG в память
            для отображения в веб-интерфейсе и запуска задач.

        База данных Airflow:
            После того как DAG загружен в память через DagBag, Airflow регистрирует его в метаданных базы данных.
            Эта база данных управляет всей внутренней информацией Airflow:
                Состоянием DAG (активен/приостановлен).
                Историей выполнения DAG и задач.
                Планированием расписаний.
                Пользовательскими настройками и другими данными.
            DAG из dags_folder автоматически добавляется в таблицу базы данных dag.

        1. DagBag:
            Показывает DAG, которые физически находятся в папке, даже если они не были зарегистрированы в базе данных.

        2. DagModel:
            Показывает, зарегистрирован ли DAG и его состояние (активен или приостановлен) в базе данных.

        Что происходит при удалении DAG-файла?
            1/DAG остаётся в базе данных после удаления файла.
            2/Он отмечается как "отсутствующий" или "отключённый" в веб-интерфейсе:
                В разделе "DAGs" вы увидите example_dag с пометкой "deactivated" или "missing".
            3/Для полной очистки используйте CLI или скрипт:
                # Удалить все DAG, чьи файлы отсутствуют
                DagModel.remove_deleted_dags()

    """

    def __init__(self):
        self.dag_bag = DagBag()

    @staticmethod
    def get_url_string(any_config: dict | str) -> URL:
        """
            Создаем URL строку:
        """

        # Проверка типа входной конфигурации подключения:
        # Если на вход конфигурация в словаре:
        if isinstance(any_config, dict) == True:
            url_string = URL.create(**any_config)  # 1. Формируем URL-строку соединения с БД.
            #  Эквивалент: url_conf_locdb = (f'{drivername}://{username}:{password}@{host}:{port}/{database}')

        # Если на вход url_conf_locdb:
        elif isinstance(any_config, str) == True:
            url_string = any_config
        else:
            url_string = None

        return url_string

    def get_engine(self, config: dict | str) -> Engine:
        # engine_mart_sv = PostgresHook(postgres_conn_id='mart_sv').get_sqlalchemy_engine()  # mart_sv postgres
        # return engine_mart_sv

        # Синхронные подключения:
        url_string = self.get_url_string(config)
        sinc_engine = create_engine(url_string)  # , echo=True
        return sinc_engine

    def get_count_dags_in_dagbag(self):
        """
            Возвращает количество dags, содержащихся в Airflow (в классе DagBag).
        """

        return self.dag_bag.size

    def get_tasks_for_dag(self, task_instances: list[TaskInstance]) -> str:
        """
            Метод возвращает строку с данными на каждый таск в конкретном даге.
            Принимает: task_instances = dag.get_task_instances() # Все задачи для DAG
        """

        task_list = []

        for task in task_instances:
            task_info = (
                f"task_id: {task.task_id},"  # task_id
                f" state: {task.state},"
                f" start_date: {task.start_date}, "
                f"end_date: {task.end_date}"
            )
            task_list.append(task_info)

        return ' ;'.join(task_list)

    def get_property_dagbag(self) -> dict[str, dict[any, any]]:
        """
            DagBag работает только с загруженными DAG-ами, но не предоставляет информацию о том,
            используются ли они или каков их статус выполнения.

            Если вы видите DAG в веб-интерфейсе Airflow, это означает, что DAG был успешно загружен и проанализирован.
            Такие DAG-и доступны через DagBag.

            Планировщик Airflow (Scheduler) сканирует файлы, находящиеся в каталоге, указанном в dags_folder.
            Эти файлы должны содержать определение DAG (например, объект класса DAG из Airflow).
            Если DAG найден, он считается загруженным.

            Если файл с определением DAG содержит ошибки или недоступен
            (например, файл ещё не синхронизирован с GitLab), он не попадёт в DagBag.

            Доступные методы:

                * dags - Возвращает словарь dag_id типа:

                        {
                            'ключ dag_id': <объект (экземпляр) класса: dag_id>,
                            'example_dag_2': <DAG: example_dag_2>,
                            'example_dag_3': <DAG: example_dag_3>
                        },

                        где
                            * Ключ: dag_id (ключ) — это текстовый идентификатор DAG, который вы задали при его создании;
                            * Значение ключа: <DAG: example_dag_1> - это строковое представление объекта DAG.
                                Оно возвращается методом __repr__() класса DAG, который определяет,
                                как объект отображается в консоли.
                                DAG — это имя класса.
                                example_dag_1 — это идентификатор DAG (dag_id).


            * dag_bag.size - Возвращает количество dag, содержащихся в DagBag.
            * dag_bag.dag_ids - Возвращает список ID DAG-ов из DagBag (почти то же самое, что и dags, только в списке).

           ==========================================================
            Основные атрибуты:
                dag_id — идентификатор DAG (строка).
                description — описание DAG (строка).
                owner — владелец DAG (строка).
                start_date — дата начала DAG (объект datetime).
                end_date — дата завершения DAG, если указана (объект datetime или None).
                schedule_interval — интервал выполнения DAG (например, timedelta, None, cron).
                tags — список тегов, связанных с DAG (список строк).
                tasks — список всех задач (объекты BaseOperator), которые определены в DAG.
                [<DummyOperator(task_id='task_1')>, <BashOperator(task_id='task_2')>]
                is_paused_upon_creation — флаг, указывает, будет ли DAG при создании в паузе (булево).
                catchup — флаг, указывает, нужно ли "догонять" пропущенные задачи (булево).
                default_args — словарь аргументов по умолчанию для всех задач DAG.

            Дополнительные атрибуты:
                fileloc — путь к файлу, где определен DAG (строка).
                last_loaded — время последней загрузки DAG (объект datetime).
                max_active_runs — максимальное количество активных запусков DAG (целое число).
                max_active_tasks — максимальное количество активных задач для DAG (целое число).
                concurrency — максимальное количество задач, которые могут быть выполнены одновременно (целое число).
                doc_md — документация в формате Markdown, связанная с DAG (строка или None).
                next_dagrun — объект datetime, представляющий время следующего запуска DAG.
                next_dagrun_data_interval_start — начало следующего интервала данных (объект datetime).
                next_dagrun_data_interval_end — конец следующего интервала данных (объект datetime).

            Пример вывода:
            # Пример словаря `result`, заполненного данными
            result = {
                "dag1": {
                    "owner": "owner1",
                    "fileloc": "/path/to/dag1",
                    "description": "This is DAG 1",
                    "tags": "tag1, tag2",
                    "last_loaded": "2024-12-18 12:00:00",
                    "next_dagrun": "2024-12-20 12:00:00",
                    "last_dagrun": "2024-12-19 12:00:00",
                    "state": "success",
                    "max_active_runs": 3,
                    "task_instances": [{"task_id": "task1", "state": "success"},
                        {"task_id": "task2", "state": "failed"}],
                    "start_date": "2024-12-10",
                    "end_date": "2024-12-20",
                    "schedule_interval": "0 12 * * *",
                },
        """

        result = {}

        # Получаем из Airflow словарь всех dag_id с dag-объектами:
        dags_dict = self.dag_bag.dags

        for dag_id, dag in dags_dict.items():
            # last_dagrun = dag.get_last_dagrun()  # Последний выполненный DAGRun | (проверить) dag_id
            # task_instances: list[TaskInstance] = dag.get_task_instances(
            #     start_date=dag.start_date, end_date=datetime.utcnow()
            # )  # Все задачи для DAG  | (проверить)

            # execution_date = last_dagrun.execution_date if last_dagrun else None
            # state = last_dagrun.state if last_dagrun else None

            # Присваиваем ключ как идентификатор дага со значениями его атрибутов:
            result[dag_id] = {
                # -------------------------------------- Основные для идентификации:
                # 'dag_display_name': dag.dag_display_name, в версии 2.4.3 нет метода.
                'dag_id': dag_id,
                'description': dag.description,  # +
                'tags': ' ,'.join(dag.tags or []),  # + (можно сохранить отдельно и проводить анализ по тегам).
                'fileloc': dag.fileloc,  # Ссылка на даг /  -
                'owner': dag.owner,  # Скорее всего один на все даги / -

                # -------------------------------------- Основные для анализа:
                'last_loaded': dag.last_loaded,  # +
                'max_active_runs': dag.max_active_runs,  # Сколько раз запускался

                # -------------------------------------- Мало значимые:
                'start_date': dag.start_date,  # Скорее всего из-за days_ago функции - мало полезный тег / + -
                'end_date': dag.end_date,
                'schedule_interval': dag.schedule_interval,

                # -------------------------------------- Разобраться:
                # 'last_dagrun': execution_date,
                # 'state': state,
                # 'tasks_for_dag': self.get_tasks_for_dag(task_instances),  # Список параметров каждого таска в даге.
            }

        return result

    def get_result_df(self) -> DataFrame:  # , result_dags_params_dict: dict
        """
            Сохраняем результаты в датафрейм.
        """

        result_dags_params_dict = self.get_property_dagbag()

        # Преобразование словаря в Pandas DataFrame
        result_df = DataFrame.from_dict(result_dags_params_dict, orient='index')

        current_time = datetime.now()

        # Форматируем время в строку
        formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Добавляем новые колонки со значением 0:
        result_df['dt_load'] = formatted_time

        return result_df

    def save_result_df_in_db(self, name_table: str, schema: str, replace_or_append, config: dict | str):
        """
            Сохраняем датафрейм в базу данных.

            * if_exists:
                fail: вызовет исключение ValueError.
                replace: удалит и снова создаст таблицу перед вставкой новых значений.
                append: вставить новые значения в существующую таблицу.
                method='multi' используется для оптимизации вставки большого объема данных.

                # con: object,  result_df: DataFrame,
        """

        result_df = self.get_result_df()
        result_df.to_sql(
            name=name_table, schema=schema, con=self.get_engine(config), if_exists=replace_or_append, index=False
        )

        return None

    # def save_comments(self, name_table: str, schema: str, ):
    #
    #     comments = TableCommentator(engine=self.get_engine(), name_table='', schema='')
    #     comments.table_comment()




@task
def dags_analyzer(name_table: str, schema: str, replace_or_append, config: dict | str):
    # con: object, , table_comments: str
    """
        Оборачиваем в таск наш клас.
    """

    dags = DagsAnalyzer()
    dags.save_result_df_in_db(
        name_table=name_table, schema=schema, replace_or_append=replace_or_append, config=config
    )

    engin_mart_sv = dags.get_engine(config)
    comments = TableCommentator(engine=engin_mart_sv, name_table=name_table, schema=schema)
    comments.table_comment('Таблица содержит выгрузку данных из Airflow по имеющимся дагам (все доступные атрибуты).')
    # 'Тестовый коммент'  # table_comments
    comments.column_comment(
        dt_load='Дата обновления таблицы в базе данных.',
        dag_id='Идентификатор DAG (строка), чаще всего совпадает с именем файла/главной функции.',
        description='Описание DAG (строка).',
        tags='Список тегов, связанных с DAG (список строк) для быстрого поиска DAG.',
        fileloc='Ссылка-путь к файлу, где определен DAG (строка), (к расположению в airflow).',
        owner='Владелец DAG (строка)',
        last_loaded='Время последней загрузки DAG (объект datetime).',  # todo: разобраться!
        # показывает время обращения к дагу а не его загрузку
        max_active_runs='Максимальное количество активных запусков DAG (целое число).',  # todo: везде показывает 1,
        # видимо последнее количество запусков за время обращения (чтения)
        start_date='Дата начала DAGа (объект datetime: datetime(2024, 12, 19) или days_ago(1)).',
        end_date='Дата завершения DAG, если указана (объект datetime или None).',  # todo: везде показан None
        # (тк не удален, надо менять логику !)
        schedule_interval='Интервал выполнения DAG (например, timedelta, None, cron).',
    )
    return None


# ======================================================================================================================
with DAG(
    dag_id='dags_analyzer',
    start_date=datetime(2024, 12, 19),  # ,  # Начало DAG days_ago(1)
    default_args={
        "owner": 'Poznyishev.AA@dns-shop.ru',
        "retries": 1,
        "retry_delay": timedelta(minutes=50),
        'on_failure_callback': bt.send_alert_telegram,
        'on_success_callback': tl.log_task_success,
    },
    description="Анализ имеющихся дагов в Airflow, их параметров, поиск неактуальных.",
    schedule='50 23 * * *',  # последний день месяца
    catchup=False,
    tags=[
        'poznyshev', 'dags_analyzer', 'analyzer', 'анализ дагов', 'поиск дагов',
    ]
) as dag:
# ==================================================================================================================
# ==================================================================================================================
    dags_analyzer(
        name_table='dags_analyzer',
        schema='audit',
        replace_or_append='replace',
        config=CONFIG_MART_SV,
        # table_comments='Тестовый коммент'
    )


