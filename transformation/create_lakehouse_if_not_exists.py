import notebookutils
def create_lakehouse_if_not_exists(lakehouse_name):
    try:
        id = notebookutils.lakehouse.get(lakehouse_name).id
    except:
        notebookutils.lakehouse.create(lakehouse_name)
        id = notebookutils.lakehouse.get(lakehouse_name).id
    return id
