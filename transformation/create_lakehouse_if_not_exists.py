import notebookutils
import sempy.fabric as fabric
def create_lakehouse_if_not_exists(lakehouse_name):
    try:
        notebookutils.lakehouse.get(lakehouse_name)
        return 1
    except:
        try:
            fabric.create_lakehouse(lakehouse_name,enable_schema=True)
            notebookutils.lakehouse.get(lakehouse_name)
            return 1
        except:
            return 0
