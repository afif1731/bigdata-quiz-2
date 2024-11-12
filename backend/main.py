from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from quart import Quart, request, jsonify
from quart_cors import cors
from middleware.custom_error import CustomError
from middleware.custom_response import CustomResponse
import asyncio
import json
import os

PORT=4000

app = Quart(__name__)
spark = SparkSession.builder.appName("BigDataModelAPI").getOrCreate()

app = cors(app, allow_origin="*")

# Fungsi untuk memuat model
def load_model(model_name):
    return ALSModel.load(f"../consument/models/{model_name}")

# Load model-model yang telah disimpan
model1 = load_model("model1")
model2 = load_model("model2")
model3 = load_model("model3")

@app.after_serving
async def shutdown():
    await spark.close()

@app.route('/', methods=['GET'])
def home():
    return 'runnin wild...'

@app.route('/recommendation', methods=['POST'])
async def RecommendationRouter():
    try:
        req = await request.get_json()
        modelName = request.args.get('model')

        if req['user_id'] is None:
            raise CustomError(400, 'user_id required')

        if modelName is None:
            raise CustomError(400, 'model query required')

        if modelName not in ['model1', 'model2', 'model3']:
            raise CustomError(400, 'invalid model name')

        user_id_df = spark.createDataFrame([(req['user_id'],)], ["user_id"])
        if modelName == 'model1':
            recommendations = model1.recommendForUserSubset(user_id_df, 5)  # Misalnya, rekomendasi 5 produk
            recommendations_json = recommendations.toPandas().to_json(orient='records')
            response = CustomResponse(200, 'get recommendation successfully', recommendations_json)
            return jsonify(response.JSON()), response.code

        elif modelName == 'model2':
            recommendations = model2.recommendForUserSubset(user_id_df, 5)  # Misalnya, rekomendasi 5 produk
            recommendations_json = recommendations.toPandas().to_json(orient='records')
            response = CustomResponse(200, 'get recommendation successfully', recommendations_json)
            return jsonify(response.JSON()), response.code

        elif modelName == 'model3':
            recommendations = model3.recommendForUserSubset(user_id_df, 5)  # Misalnya, rekomendasi 5 produk
            recommendations_json = recommendations.toPandas().to_json(orient='records')
            response = CustomResponse(200, 'get recommendation successfully', recommendations_json)
            return jsonify(response.JSON()), response.code

        else:
            raise CustomError(400, 'invalid model name')
    except Exception as err:
        print(err)
        return jsonify({"error": json.dumps(err)}),500


if __name__ == '__main__':
    asyncio.run(app.run(host='159.89.203.127', port=PORT, debug=True))