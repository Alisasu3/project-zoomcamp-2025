
DATASET_URL="https://www.kaggle.com/datasets/alexlau203/sydney-house-prices"

echo "Downloading dataset from Kaggle..."

kaggle datasets download -d alexlau203/sydney-house-prices

ZIP_FILE=$(ls *.zip)

if [[ ! -f "$ZIP_FILE" ]]; then
  echo "Error: No zip file found"
  exit 1
fi

echo "Unzipping $ZIP_FILE..."

unzip "$ZIP_FILE" -d ./data

# List contents of the unzipped directory
echo "Unzipped files:"
ls ./data

# Clean up by removing the zip file after extraction
rm "$ZIP_FILE"

echo "Done."
