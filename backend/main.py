from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from quart import Quart, request, jsonify
from quart_cors import cors
from middleware.custom_error import CustomError
from middleware.custom_response import CustomResponse
import asyncio
import json

PORT=4000

app = Quart(__name__)
spark = SparkSession.builder.appName("BigDataModelAPI").getOrCreate()

app = cors(app, allow_origin="*")

# Fungsi untuk memuat model
def load_model(model_name):
    return ALSModel.load(f"../consument/models/{model_name}")

# Load model-model yang telah disimpan
model1 = load_model("model1.1")
model2 = load_model("model2.1")
model3 = load_model("model3.1")

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

        elif modelName == 'model2':
            recommendations = model2.recommendForUserSubset(user_id_df, 5)  # Misalnya, rekomendasi 5 produk

        elif modelName == 'model3':
            recommendations = model3.recommendForUserSubset(user_id_df, 5)  # Misalnya, rekomendasi 5 produk
        
        recommendations_json = recommendations.toPandas().to_json(orient='records')
        parsed_recommendations = json.loads(recommendations_json)
        only_recommendations = parsed_recommendations[0]['recommendations'] if parsed_recommendations else []
        formatted_recommendations = [
            {"product_id": str(item[0]), "accuracy": item[1]}
            for item in only_recommendations
        ]
        response = CustomResponse(200, 'get recommendation successfully', formatted_recommendations)
        return jsonify(response.JSON()), response.code
    except Exception as err:
        print(err)
        return jsonify(err.JSON()),err.code

@app.route('batch-recommendation', methods=['POST'])
async def BatchRecommendationRouter():
    try:
        req = await request.get_json()
        modelName = request.args.get('model')

        if req['user_ids'] is None:
            raise CustomError(400, 'user_id required')

        if modelName is None:
            raise CustomError(400, 'model query required')

        if modelName not in ['model1', 'model2', 'model3']:
            raise CustomError(400, 'invalid model name')
        
        result_list = []
        for user_id in req['user_ids']:
            user_id_df = spark.createDataFrame([(user_id,)], ["user_id"])
            if modelName == 'model1':
                recommendations = model1.recommendForUserSubset(user_id_df, 5)  # Misalnya, rekomendasi 5 produk

            elif modelName == 'model2':
                recommendations = model2.recommendForUserSubset(user_id_df, 5)  # Misalnya, rekomendasi 5 produk

            elif modelName == 'model3':
                recommendations = model3.recommendForUserSubset(user_id_df, 5)  # Misalnya, rekomendasi 5 produk
            
            recommendations_json = recommendations.toPandas().to_json(orient='records')
            parsed_recommendations = json.loads(recommendations_json)
            only_recommendations = parsed_recommendations[0]['recommendations'] if parsed_recommendations else []
            formatted_recommendations = {
                "user_id": str(user_id),
                "recommendations": [
                    {"product_id": str(item[0]), "accuracy": item[1]}
                    for item in only_recommendations
                ]
            }
            result_list.append(formatted_recommendations)
        
        response = CustomResponse(200, 'get batch recommendation successfully', result_list)
        return jsonify(response.JSON()), response.code
    except Exception as err:
        print(err)
        return jsonify(err.JSON()),err.code

@app.route('one-recommendation', methods=['GET'])
async def OneRecommendationRouter():
    try:
        user_id = request.args.get('user_id')
        
        if user_id is None:
            raise CustomError(400, 'user_id required')
        
        user_id_df = spark.createDataFrame([(user_id,)], ["user_id"])

        recommendations_1 = model1.recommendForUserSubset(user_id_df, 5)  # Misalnya, rekomendasi 5 produk
        recommendations_2 = model2.recommendForUserSubset(user_id_df, 5)  # Misalnya, rekomendasi 5 produk
        recommendations_3 = model3.recommendForUserSubset(user_id_df, 5)  # Misalnya, rekomendasi 5 produk

        recommendations_1_json = json.loads(recommendations_1.toPandas().to_json(orient='records'))
        recommendations_2_json = json.loads(recommendations_2.toPandas().to_json(orient='records'))
        recommendations_3_json = json.loads(recommendations_3.toPandas().to_json(orient='records'))

        only_recommendations_1 = recommendations_1_json[0]['recommendations'] if recommendations_1_json else []
        only_recommendations_2 = recommendations_2_json[0]['recommendations'] if recommendations_2_json else []
        only_recommendations_3 = recommendations_3_json[0]['recommendations'] if recommendations_3_json else []
        
        formatted_recommendations_1 = {
            "model": "model1",
            "recommendations": [
                {"product_id": str(item[0]), "accuracy": item[1]}
                for item in only_recommendations_1
            ]
        }
        formatted_recommendations_2 = {
            "model": "model1",
            "recommendations": [
                {"product_id": str(item[0]), "accuracy": item[1]}
                for item in only_recommendations_2
            ]
        }
        formatted_recommendations_3 = {
            "model": "model1",
            "recommendations": [
                {"product_id": str(item[0]), "accuracy": item[1]}
                for item in only_recommendations_3
            ]
        }

        response = CustomResponse(200, 'get batch recommendation successfully', [formatted_recommendations_1, formatted_recommendations_2, formatted_recommendations_3])
        return jsonify(response.JSON()), response.code
    except Exception as err:
        print(err)
        return jsonify(err.JSON()),err.code

if __name__ == '__main__':
    asyncio.run(app.run(host='159.89.203.127', port=PORT, debug=True))