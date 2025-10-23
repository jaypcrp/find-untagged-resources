# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.12

# Copy code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set handler (file_name.function_name)
CMD ["lambda_function.lambda_handler"]
