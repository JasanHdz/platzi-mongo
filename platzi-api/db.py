from bson.json_util import dumps, ObjectId
from flask import current_app
from pymongo import MongoClient, DESCENDING
from werkzeug.local import LocalProxy


# Este método se encarga de configurar la conexión con la base de datos
def get_db():
    platzi_db = current_app.config['PLATZI_DB_URI']
    client = MongoClient(platzi_db)
    return client.platzi


# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)


def test_connection():
    return dumps(db.collection_names())


def collection_stats(collection_nombre):
    return dumps(db.command('collstats', collection_nombre))

# -----------------Carreras-------------------------


def crear_carrera(json):
    return str(db.carreras.insert_one(json).inserted_id)


def consultar_carrera_por_id(carrera_id):
    return dumps(db.carreras.find_one({'_id': ObjectId(carrera_id)}))


def actualizar_carrera(carrera):
    # Esta funcion solamente actualiza nombre y descripcion de la carrera
    return str(db.carreras.update_one({'_id': ObjectId(carrera['id'])}, {'$set': {'nombre': carrera['nombre'], 'description': carrera['description']}}).modified_count)


def borrar_carrera_por_id(carrera_id):
    return str(db.carreras.delete_one({'_id': ObjectId(carrera_id)}))


# Clase de operadores
def consultar_carreras(skip, limit):
    return dumps(db.carreras.find({}).skip(int(skip)).limit(int(limit)))


def agregar_curso(json):
    curso = consultar_curso_por_id_proyeccion(
        json['id_curso'], proyeccion={'nombre': 1})
    return str(db.carreras.update_one({'_id': ObjectId(json['id_carrera'])},
                                      {'$addToSet': {'cursos': curso}}).modified_count)


def borrar_curso_de_carrera(json):
    filtro = {'_id': ObjectId(json['id_carrera'])}
    remove = {'$pull': {'cursos': {'_id': ObjectId(json['id_curso'])}}}
    return str(db.carreras.update_one(filtro, remove).modified_count)

# -----------------Cursos-------------------------
# db.carreras.update_one({'_id': ObjectId("5e3fb461b68af31c14553a1b")},  {'$pull': {'cursos': {'_id': ObjectId("5e3fb601b68af31114047388")}}).modified_count


def crear_curso(json):
    return str(db.cursos.insert_one(json).inserted_id)


def consultar_curso_por_id(id_curso):
    return dumps(db.cursos.find_one({'_id': ObjectId(id_curso)}))


def actualizar_curso(curso):
    # Esta funcion solamente actualiza nombre, descripcion y clases del curso
    return str(db.cursos.update_one({'_id': ObjectId(curso['id'])},
                                    {'$set': {'nombre': curso['nombre'],
                                              'description': curso['description'],
                                              'clases': curso['clases']}}).modified_count)


def borrar_curso_por_id(curso_id):
    return str(db.cursos.delete_one({'_id': ObjectId(curso_id)}).deleted_count)


def consultar_curso_por_id_proyeccion(id_curso, proyeccion=None):
    return str(db.cursos.find_one({'_id': ObjectId(id_curso)}, proyeccion))


def consultar_curso_por_nombre(nombre):
    return dumps(db.cursos.find({'$text': {'$search': nombre}}))
