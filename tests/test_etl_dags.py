"""Integration tests for API-backed ETL DAGs."""
import pytest
from unittest.mock import patch, MagicMock
from dags.auto_insurance_etl import auto_insurance_etl
from dags.life_insurance_etl import life_insurance_etl
from dags.nonlife_insurance_etl import nonlife_insurance_etl


class TestAutoInsuranceETL:
    """Tests for auto_insurance_etl DAG."""

    def test_auto_etl_dag_structure(self):
        """Verify auto ETL DAG is properly defined."""
        dag = auto_insurance_etl()
        assert dag is not None
        assert dag.dag_id == "auto_insurance_etl"
        assert dag.schedule == "0 2 * * *"
        assert "etl" in dag.tags
        assert "auto" in dag.tags

    def test_auto_contract_task_exists(self):
        """Verify load_contract_info task exists."""
        dag = auto_insurance_etl()
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_contract_info" in task_ids

    def test_auto_loss_task_exists(self):
        """Verify load_loss_status task exists."""
        dag = auto_insurance_etl()
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_loss_status" in task_ids

    def test_auto_victim_task_exists(self):
        """Verify load_victim_info task exists."""
        dag = auto_insurance_etl()
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_victim_info" in task_ids

    def test_auto_contract_table_structure(self, pg_hook):
        """Verify fact_auto_contract table has correct schema."""
        cursor = pg_hook.get_conn().cursor()
        try:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'fact_auto_contract'
            """)
            columns = {col[0] for col in cursor.fetchall()}

            expected = {"date_key", "ins_type", "coverage", "sex", "age_group",
                       "origin", "car_type", "join_cnt", "elapsed_premium"}
            assert expected.issubset(columns)
        finally:
            cursor.close()

    @patch('dags.insurance_dw_common.fetch_table_items')
    @patch('dags.insurance_dw_common.replace_table_rows')
    def test_auto_contract_load(self, mock_replace, mock_fetch, pg_hook):
        """Test auto contract data loading."""
        mock_fetch.return_value = [
            {
                "insTypeNm": "자동차",
                "covgNm": "종합",
                "sexNm": "남",
                "ageNm": "30대",
                "domesticImportNm": "국산",
                "carTypeNm": "승용차",
                "joinCnt": "100",
                "elpsPrm": "5000"
            }
        ]
        mock_replace.return_value = 1

        dag = auto_insurance_etl()
        assert dag is not None


class TestLifeInsuranceETL:
    """Tests for life_insurance_etl DAG."""

    def test_life_etl_dag_structure(self):
        """Verify life ETL DAG is properly defined."""
        dag = life_insurance_etl()
        assert dag is not None
        assert dag.dag_id == "life_insurance_etl"
        assert dag.schedule == "15 2 * * *"
        assert "etl" in dag.tags
        assert "life" in dag.tags

    def test_life_general_task_exists(self):
        """Verify load_general_status task exists."""
        dag = life_insurance_etl()
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_general_status" in task_ids

    def test_life_finance_task_exists(self):
        """Verify load_finance_status task exists."""
        dag = life_insurance_etl()
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_finance_status" in task_ids

    def test_life_business_task_exists(self):
        """Verify load_business_activity task exists."""
        dag = life_insurance_etl()
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_business_activity" in task_ids

    def test_life_kpi_task_exists(self):
        """Verify load_management_kpi task exists."""
        dag = life_insurance_etl()
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_management_kpi" in task_ids

    def test_life_general_table_structure(self, pg_hook):
        """Verify fact_life_general table has correct schema."""
        cursor = pg_hook.get_conn().cursor()
        try:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'fact_life_general'
            """)
            columns = {col[0] for col in cursor.fetchall()}

            expected = {"date_key", "company_name", "title", "employee_type",
                       "person_cnt", "employee_cnt"}
            assert expected.issubset(columns)
        finally:
            cursor.close()

    def test_life_finance_table_structure(self, pg_hook):
        """Verify fact_life_finance table has correct schema."""
        cursor = pg_hook.get_conn().cursor()
        try:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'fact_life_finance'
            """)
            columns = {col[0] for col in cursor.fetchall()}

            expected = {"date_key", "company_name", "account_code", "account_name",
                       "amount", "ratio"}
            assert expected.issubset(columns)
        finally:
            cursor.close()


class TestNonlifeInsuranceETL:
    """Tests for nonlife_insurance_etl DAG."""

    def test_nonlife_etl_dag_structure(self):
        """Verify nonlife ETL DAG is properly defined."""
        dag = nonlife_insurance_etl()
        assert dag is not None
        assert dag.dag_id == "nonlife_insurance_etl"
        assert dag.schedule == "30 2 * * *"
        assert "etl" in dag.tags
        assert "nonlife" in dag.tags

    def test_nonlife_general_task_exists(self):
        """Verify load_general_status task exists."""
        dag = nonlife_insurance_etl()
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_general_status" in task_ids

    def test_nonlife_finance_task_exists(self):
        """Verify load_finance_status task exists."""
        dag = nonlife_insurance_etl()
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_finance_status" in task_ids

    def test_nonlife_business_task_exists(self):
        """Verify load_business_activity task exists."""
        dag = nonlife_insurance_etl()
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_business_activity" in task_ids

    def test_nonlife_kpi_task_exists(self):
        """Verify load_management_kpi task exists."""
        dag = nonlife_insurance_etl()
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_management_kpi" in task_ids

    def test_nonlife_general_table_structure(self, pg_hook):
        """Verify fact_nonlife_general table has correct schema."""
        cursor = pg_hook.get_conn().cursor()
        try:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'fact_nonlife_general'
            """)
            columns = {col[0] for col in cursor.fetchall()}

            expected = {"date_key", "company_name", "title", "employee_type",
                       "person_cnt", "employee_cnt"}
            assert expected.issubset(columns)
        finally:
            cursor.close()

    def test_nonlife_business_table_structure(self, pg_hook):
        """Verify fact_nonlife_business table has correct schema."""
        cursor = pg_hook.get_conn().cursor()
        try:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'fact_nonlife_business'
            """)
            columns = {col[0] for col in cursor.fetchall()}

            expected = {"date_key", "company_name", "ins_type_code", "ins_type_name", "amount"}
            assert expected.issubset(columns)
        finally:
            cursor.close()


class TestETLScheduling:
    """Tests for ETL scheduling and execution order."""

    def test_scheduling_order(self):
        """Verify ETL DAGs have staggered schedules."""
        auto_dag = auto_insurance_etl()
        life_dag = life_insurance_etl()
        nonlife_dag = nonlife_insurance_etl()

        # Should run at: 02:00, 02:15, 02:30
        assert auto_dag.schedule == "0 2 * * *"
        assert life_dag.schedule == "15 2 * * *"
        assert nonlife_dag.schedule == "30 2 * * *"

    def test_catchup_disabled(self):
        """Verify catchup is disabled on all ETL DAGs."""
        assert not auto_insurance_etl().catchup
        assert not life_insurance_etl().catchup
        assert not nonlife_insurance_etl().catchup

    def test_start_dates_set(self):
        """Verify start dates are set correctly."""
        assert auto_insurance_etl().start_date is not None
        assert life_insurance_etl().start_date is not None
        assert nonlife_insurance_etl().start_date is not None
