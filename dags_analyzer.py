"""
    Анализ имеющихся дагов в Airflow, их параметров, поиск неактуальных.
"""

from datetime import datetime, timedelta
# ---------------------------------- airflow
from airflow.models import DagBag  # , DagModel, DagRun
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
# ---------------------------------- Импорт сторонних библиотек
from pandas import DataFrame
# -------------------------------- Локальные модули
import bot_alert_telegram as bt
import task_logger as tl


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
        dag_bag = DagBag()
        # con = self.engine_mart_sv()
        # dag_models = DagModel
        # dag_runs = DagRun - это модель таблицы.
        # task_instances = TaskInstance
        # Если нужно анализировать задачи не точно, надо разбираться! только для управления

    def engine_mart_sv(self):
        engine_mart_sv = PostgresHook(postgres_conn_id='mart_sv').get_sqlalchemy_engine()  # mart_sv postgres
        return engine_mart_sv

    def get_count_dags_in_dagbag(self):
        """
            Возвращает количество dags, содержащихся в Airflow (в классе DagBag).
        """

        return self.dag_bag.size

    def get_tasks_for_dag(self, task_instances: object) -> str:
        """
            Метод возвращает строку с данными на каждый таск в конкретном даге.
            Принимает: task_instances = dag.get_task_instances() # Все задачи для DAG
        """

        task_list = []

        for task in task_instances:
            task_info = (
                f"task_id: {task.task_id},"
                f" state: {task.state},"
                f" execution_date: {task.execution_date},"
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
            last_dagrun = dag.get_last_dagrun()  # Последний выполненный DAGRun
            task_instances = dag.get_task_instances()  # Все задачи для DAG

            # Присваиваем ключ как идентификатор дага со значениями его атрибутов:
            result[dag_id] = {
                # -------------------------------------- Основные для идентификации:
                'owner': dag.owner,  # Скорее всего один на все даги / -
                'fileloc': dag.fileloc,  # Ссылка на даг /  -

                'description': dag.description,  # +
                'tags': ' ,'.join(dag.tags),  # + (можно сохранить отдельно и проводить анализ по тегам).

                # -------------------------------------- Основные для анализа:
                'last_loaded': dag.last_loaded,  # +
                'next_dagrun': dag.next_dagrun,  # + Указывает следующую запланированную дату выполнения DAG.
                'last_dagrun': last_dagrun.execution_date,
                'state': last_dagrun.state,
                'max_active_runs': dag.max_active_runs,  # Сколько раз запускался

                'task_instances': self.get_tasks_for_dag(task_instances),  # Список параметров каждого таска в даге.

                # -------------------------------------- Мало значимые:
                'start_date': dag.start_date,  # Скорее всего из-за days_ago функции - мало полезный тег / + -
                'end_date': dag.end_date,
                'schedule_interval': dag.schedule_interval,
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

    def save_result_df_in_db(self, name_table: str, schema: str, replace_or_append):
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
            name=name_table, schema=schema, con=self.engine_mart_sv(), if_exists=replace_or_append, index=False
        )

        return None


@task
def dags_analyzer(name_table: str, schema: str, replace_or_append):  # con: object,
    """
        Оборачиваем в таск наш клас.
    """

    dags_analyzer = DagsAnalyzer()
    dags_analyzer.save_result_df_in_db(name_table=name_table, schema=schema, replace_or_append=replace_or_append)
    return None


# ======================================================================================================================
with DAG(
    dag_id='dags_analyzer',
    start_date=datetime(2024, 12, 19),  # ,  # Начало DAG days_ago(1)
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=50),
        'on_failure_callback': bt.send_alert_telegram,
        'on_success_callback': tl.log_task_success,
    },
    description="Анализ имеющихся дагов в Airflow, их параметров, поиск неактуальных.",
    schedule_interval=None,  # последний день месяца
    catchup=False,
    tags=[
        'poznyshev', 'dags_analyzer', 'analyzer', 'анализ дагов', 'поиск дагов',
    ]
) as dag:
# ==================================================================================================================
# ==================================================================================================================
    dags_analyzer(name_table='dags_analyzer', schema='audit', replace_or_append='replace')
