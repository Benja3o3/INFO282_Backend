from ..models.comuna_model import Comuna


def getComunas():
    return "Getting comunas"
    # comunas = Comuna.query.all()
    # comunas_list = []
    # for comuna in comunas:
    #     comuna_data = {
    #         "id": comuna.id,
    #         "nombre": comuna.nombre,
    #     }
    #     comunas_list.append(comuna_data)
    # return comunas_list
