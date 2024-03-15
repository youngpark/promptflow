import re
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from promptflow._utils._errors import InvalidImageInput, LoadMultimediaDataError
from promptflow._utils.multimedia_utils import (
    BasicMultimediaProcessor,
    ImageProcessor,
    MultimediaProcessor,
    OpenaiVisionMultimediaProcessor,
    TextProcessor,
    _process_recursively,
)
from promptflow.contracts.flow import FlowInputDefinition
from promptflow.contracts.multimedia import Image, PFBytes, Text
from promptflow.contracts.tool import ValueType

from ...utils import DATA_ROOT, FLOW_ROOT, get_flow_folder

TEST_IMAGE_PATH = DATA_ROOT / "logo.jpg"


@pytest.mark.unittest
class TestImageProcessor:
    def test_get_extension_from_mime_type(self):
        mime_type = "image/jpeg"
        result = ImageProcessor.get_extension_from_mime_type(mime_type)
        assert result == "jpeg"

        mime_type = "image/*"
        result = ImageProcessor.get_extension_from_mime_type(mime_type)
        assert result is None

    def test_get_multimedia_info(self):
        key = "data:image/jpeg;base64"
        result = ImageProcessor.get_multimedia_info(key)
        assert result == ("jpeg", "base64")

        key = "invalid"
        result = ImageProcessor.get_multimedia_info(key)
        assert result == (None, None)

    def test_is_url(self):
        url = "http://example.com"
        result = ImageProcessor.is_url(url)
        assert result is True

        url = "not a url"
        result = ImageProcessor.is_url(url)
        assert result is False

    def test_is_base64(self):
        base64_str = "data:image/jpeg;base64,/9j/12345ABC"
        result = ImageProcessor.is_base64(base64_str)
        assert result is True

        base64_str = "/9j/12345ABC"
        result = ImageProcessor.is_base64(base64_str)
        assert result is True

        base64_str = "not a base64 string"
        result = ImageProcessor.is_base64(base64_str)
        assert result is False

    def test_create_image_from_file(self):
        image = ImageProcessor.create_image_from_file(TEST_IMAGE_PATH)
        assert isinstance(image, Image)
        assert image._mime_type == "image/jpeg"

    @pytest.mark.parametrize("image_path", ["logo.jpg", "logo.png", "logo.webp", "logo.gif"])
    def test_create_image_from_base64(self, image_path):
        image = ImageProcessor.create_image_from_file(DATA_ROOT / image_path)
        base64_str = image.to_base64()
        image_from_base64 = ImageProcessor.create_image_from_base64(base64_str)
        assert str(image) == str(image_from_base64)
        format = image_path.split(".")[-1]
        mime_type = f"image/{format}" if format != "jpg" else "image/jpeg"
        assert mime_type == image_from_base64._mime_type

    @patch("requests.get")
    def test_create_image_from_url_with_mime_type(self, mock_get):
        url = "https://example.com/image.jpg"
        content = b"image content"
        mime_type = "image/jpeg"
        mock_get.return_value = MagicMock(status_code=200, content=content)

        image = ImageProcessor.create_image_from_url(url, mime_type)

        assert isinstance(image, Image)
        assert image._mime_type == mime_type
        assert image.source_url == url

    @patch("requests.get")
    def test_create_image_from_url_failure(self, mock_get):
        url = "https://example.com/image.jpg"
        message = "Failed to fetch image"
        code = 404
        mock_get.return_value = MagicMock(status_code=code, text=message)

        with pytest.raises(InvalidImageInput) as ex:
            ImageProcessor.create_image_from_url(url)

        expected_message = f"Failed to fetch image from URL: {url}. Error code: {code}. Error message: {message}."
        assert str(ex.value) == expected_message


@pytest.mark.unittest
class TestTextProcessor:
    def test_is_text_dict_true(self):
        text_dict = {"type": "text", "text": "Hello, World!"}
        assert TextProcessor.is_text_dict(text_dict) is True

        text_dict = {"type": "text", "content": "Hello, World!"}
        assert TextProcessor.is_text_dict(text_dict) is False

        text_dict = {"type": "text", "text": {"value": "Hello, World!"}}
        assert TextProcessor.is_text_dict(text_dict) is True

        text_dict = {"type": "text", "text": {"content": "Hello, World!"}}
        assert TextProcessor.is_text_dict(text_dict) is False

    def test_create_text_from_dict(self):
        text_dict = {"type": "text", "text": "Hello, World!"}
        result = TextProcessor.create_text_from_dict(text_dict)
        assert isinstance(result, Text)


@pytest.mark.unittest
class TestMultimediaProcessor:
    @pytest.mark.parametrize(
        "message_format_type, processor_class",
        [
            ("basic", BasicMultimediaProcessor),
            ("OPENAI_VISION", OpenaiVisionMultimediaProcessor),
            ("openai_vision", OpenaiVisionMultimediaProcessor),
            (None, BasicMultimediaProcessor),
        ],
    )
    def test_create(self, message_format_type, processor_class):
        MultimediaProcessor.active_instance
        MultimediaProcessor.create(message_format_type)
        processor = MultimediaProcessor.get_instance()
        assert isinstance(processor, processor_class)

    @pytest.mark.parametrize(
        "flow_folder_name, flow_file, processor_class",
        [
            ("chat_flow_with_openai_vision_image", "flow.dag.yaml", OpenaiVisionMultimediaProcessor),
            ("chat_flow_with_image", "flow.dag.yaml", BasicMultimediaProcessor),
            ("chat_flow_with_openai_vision_image", "mock_chat.py", BasicMultimediaProcessor),
            (None, None, BasicMultimediaProcessor),
        ],
    )
    def test_create_from_yaml(self, flow_folder_name, flow_file, processor_class):
        flow_folder = get_flow_folder(flow_folder_name, FLOW_ROOT) if flow_folder_name else None
        MultimediaProcessor.create_from_yaml(flow_file, working_dir=flow_folder)
        processor = MultimediaProcessor.get_instance()
        assert isinstance(processor, processor_class)

    def test_get_instance_without_init(self):
        processor = MultimediaProcessor.get_instance()
        assert isinstance(processor, BasicMultimediaProcessor)

    def test_process_multimedia_dict_recursively(self):
        def process_func(image_dict):
            return "image_placeholder"

        image_dict = {"data:image/jpg;path": "logo.jpg"}
        value = {
            "image": image_dict,
            "images": [image_dict, image_dict],
            "object": {"image": image_dict, "other_data": "other_data"},
        }
        updated_value = MultimediaProcessor._process_multimedia_dict_recursively(
            value, {BasicMultimediaProcessor.is_multimedia_dict: process_func}
        )
        assert updated_value == {
            "image": "image_placeholder",
            "images": ["image_placeholder", "image_placeholder"],
            "object": {"image": "image_placeholder", "other_data": "other_data"},
        }


@pytest.mark.unittest
class TestBasicMultimediaProcessor:
    processor = BasicMultimediaProcessor()

    def test_is_multimedia_dict(self):
        multimedia_dict = {"data:image/jpg;path": "test.jpg"}
        assert self.processor.is_multimedia_dict(multimedia_dict) is True

        multimedia_dict = {"data:image/jpg;path": "test.jpg", "extra": "data"}
        assert self.processor.is_multimedia_dict(multimedia_dict) is False

        multimedia_dict = {}
        assert self.processor.is_multimedia_dict(multimedia_dict) is False

    def test_create_image_with_dict(self, mocker):
        ## From path
        image_dict = {"data:image/jpg;path": TEST_IMAGE_PATH}
        image_from_path = self.processor.create_image(image_dict)
        assert image_from_path._mime_type == "image/jpg"

        ## From base64
        image_dict = {"data:image/jpg;base64": image_from_path.to_base64()}
        image_from_base64 = self.processor.create_image(image_dict)
        assert str(image_from_path) == str(image_from_base64)
        assert image_from_base64._mime_type == "image/jpg"

        ## From url
        mocker.patch("requests.get", return_value=mocker.Mock(content=image_from_path, status_code=200))
        image_dict = {"data:image/jpg;url": ""}
        image_from_url = self.processor.create_image(image_dict)
        assert str(image_from_path) == str(image_from_url)
        assert image_from_url._mime_type == "image/jpg"

        mocker.patch("requests.get", return_value=mocker.Mock(content=None, status_code=404))
        with pytest.raises(InvalidImageInput) as ex:
            self.processor.create_image(image_dict)
        assert "Failed to fetch image from URL" in ex.value.message_format

    def test_create_image_with_string(self, mocker):
        ## From path
        image_from_path = self.processor.create_image(str(TEST_IMAGE_PATH))
        assert image_from_path._mime_type == "image/jpeg"

        # From base64
        image_from_base64 = self.processor.create_image(image_from_path.to_base64())
        assert str(image_from_path) == str(image_from_base64)
        assert image_from_base64._mime_type == "image/jpeg"

        ## From url
        mocker.patch("promptflow._utils.multimedia_utils.ImageProcessor.is_url", return_value=True)
        mocker.patch("promptflow._utils.multimedia_utils.ImageProcessor.is_base64", return_value=False)
        mocker.patch("requests.get", return_value=mocker.Mock(content=image_from_path, status_code=200))
        image_from_url = self.processor.create_image("Test")
        assert str(image_from_path) == str(image_from_url)
        assert image_from_url._mime_type == "image/jpeg"

        ## From image
        image_from_image = self.processor.create_image(image_from_path)
        assert str(image_from_path) == str(image_from_image)

    def test_create_image_with_invalid_cases(self):
        # Test invalid input type
        with pytest.raises(InvalidImageInput) as ex:
            self.processor.create_image(0)
        assert "Unsupported image input type" in ex.value.message_format

        # Test invalid image dict
        with pytest.raises(InvalidImageInput) as ex:
            invalid_image_dict = {"invalid_image": "invalid_image"}
            self.processor.create_image(invalid_image_dict)
        assert "Invalid image input format" in ex.value.message_format

        # Test none or empty input value
        with pytest.raises(InvalidImageInput) as ex:
            self.processor.create_image(None)
        assert "Unsupported image input type" in ex.value.message_format

        with pytest.raises(InvalidImageInput) as ex:
            self.processor.create_image("")
        assert "The image input should not be empty." in ex.value.message_format

    def test_default_json_encoder(self):
        obj = PFBytes(b"test", "image/jpg", None)
        result = self.processor.default_json_encoder(obj)
        assert result == "b'test'"

        obj = Text("test")
        with pytest.raises(TypeError) as e:
            result = self.processor.default_json_encoder(obj)
        assert "Object of type Text is not JSON serializable" in str(e.value)

    def test_load_multimedia_data(self):
        # Case 1: Test normal node
        inputs = {
            "image": FlowInputDefinition(type=ValueType.IMAGE),
            "images": FlowInputDefinition(type=ValueType.LIST),
            "object": FlowInputDefinition(type=ValueType.OBJECT),
        }
        image_dict = {"data:image/jpg;path": str(TEST_IMAGE_PATH)}
        line_inputs = {
            "image": image_dict,
            "images": [image_dict, image_dict],
            "object": {"image": image_dict, "other_data": "other_data"},
        }
        updated_inputs = self.processor.load_multimedia_data(inputs, line_inputs)
        image = ImageProcessor.create_image_from_file(TEST_IMAGE_PATH)
        assert updated_inputs == {
            "image": image,
            "images": [image, image],
            "object": {"image": image, "other_data": "other_data"},
        }

        # Case 2: Test aggregation node
        line_inputs = {
            "image": [image_dict, image_dict],
            "images": [[image_dict, image_dict], [image_dict]],
            "object": [{"image": image_dict, "other_data": "other_data"}, {"other_data": "other_data"}],
        }
        updated_inputs = self.processor.load_multimedia_data(inputs, line_inputs)
        assert updated_inputs == {
            "image": [image, image],
            "images": [[image, image], [image]],
            "object": [{"image": image, "other_data": "other_data"}, {"other_data": "other_data"}],
        }

        # Case 3: Test invalid input type
        with pytest.raises(LoadMultimediaDataError) as ex:
            line_inputs = {"image": 0}
            self.processor.load_multimedia_data(inputs, line_inputs)
        assert (
            "Failed to load image for input 'image': " "(InvalidImageInput) Unsupported image input type"
        ) in ex.value.message

    def test_resolve_multimedia_data_recursively(self):
        image_dict = {"data:image/jpg;path": "logo.jpg"}
        value = {
            "image": image_dict,
            "images": [image_dict, image_dict],
            "object": {"image": image_dict, "other_data": "other_data"},
        }
        input_dir = TEST_IMAGE_PATH
        updated_value = self.processor.resolve_multimedia_data_recursively(input_dir, value)
        updated_image_dict = {"data:image/jpg;path": str(DATA_ROOT / "logo.jpg")}
        assert updated_value == {
            "image": updated_image_dict,
            "images": [updated_image_dict, updated_image_dict],
            "object": {"image": updated_image_dict, "other_data": "other_data"},
        }

    def test_persist_multimedia_date(self, mocker):
        image = ImageProcessor.create_image_from_file(TEST_IMAGE_PATH)
        mocker.patch("builtins.open", mock_open())
        data = {"image": image, "images": [image, image, "other_data"], "other_data": "other_data"}
        persisted_data = self.processor.persist_multimedia_data(data, base_dir=Path(__file__).parent)
        file_name = re.compile(r"^[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}.jpeg$")
        assert re.match(file_name, persisted_data["image"]["data:image/jpeg;path"])
        assert re.match(file_name, persisted_data["images"][0]["data:image/jpeg;path"])
        assert re.match(file_name, persisted_data["images"][1]["data:image/jpeg;path"])

    def test_convert_multimedia_date_to_base64(self):
        image = ImageProcessor.create_image_from_file(TEST_IMAGE_PATH)
        data = {"image": image, "images": [image, image, "other_data"], "other_data": "other_data"}
        base64_data = self.processor.convert_multimedia_data_to_base64_dict(data)
        excepted_image = {f"data:{image._mime_type};base64": image.to_base64()}
        assert base64_data == {
            "image": excepted_image,
            "images": [excepted_image, excepted_image, "other_data"],
            "other_data": "other_data",
        }


@pytest.mark.unittest
class TestOpenaiVisionMultimediaProcessor:
    processor = OpenaiVisionMultimediaProcessor()

    def test_is_multimedia_dict(self):
        multimedia_dict = {"type": "image_url", "image_url": "data"}
        assert self.processor.is_multimedia_dict(multimedia_dict) is True

        multimedia_dict = {"image_url": "data"}
        assert self.processor.is_multimedia_dict(multimedia_dict) is False

        multimedia_dict = {}
        assert self.processor.is_multimedia_dict(multimedia_dict) is False

        multimedia_dict = {"image/jpeg": "test.jpg", "extra": "data"}
        assert self.processor.is_multimedia_dict(multimedia_dict) is False

        multimedia_dict = {"type": "text", "text": "data"}
        assert self.processor.is_multimedia_dict(multimedia_dict) is False

        multimedia_dict = {"type": "image_url", "image_file": "data"}
        assert self.processor.is_multimedia_dict(multimedia_dict) is False

    def test_create_image_with_dict(self, mocker):
        ## From path
        image_dict = {"type": "image_file", "image_file": {"path": TEST_IMAGE_PATH}}
        image_from_path = self.processor.create_image(image_dict)
        assert image_from_path._mime_type == "image/jpeg"

        ## From base64
        image_dict = {"type": "image_url", "image_url": {"url": image_from_path.to_base64(with_type=True)}}
        image_from_base64 = self.processor.create_image(image_dict)
        assert str(image_from_path) == str(image_from_base64)
        assert image_from_base64._mime_type == "image/jpeg"

        ## From url
        mocker.patch("requests.get", return_value=mocker.Mock(content=image_from_path, status_code=200))
        image_dict = {"type": "image_url", "image_url": {"url": "http://example.com"}}
        image_from_url = self.processor.create_image(image_dict)
        assert str(image_from_path) == str(image_from_url)
        assert image_from_url._mime_type == "image/jpeg"

        mocker.patch("requests.get", return_value=mocker.Mock(content=None, status_code=404))
        with pytest.raises(InvalidImageInput) as ex:
            self.processor.create_image(image_dict)
        assert "Failed to fetch image from URL" in ex.value.message_format

    def test_create_image_with_string(self, mocker):
        ## From path
        image_from_path = self.processor.create_image(str(TEST_IMAGE_PATH))
        assert image_from_path._mime_type == "image/jpeg"

        # From base64
        image_from_base64 = self.processor.create_image(image_from_path.to_base64())
        assert str(image_from_path) == str(image_from_base64)
        assert image_from_base64._mime_type == "image/jpeg"

        ## From url
        mocker.patch("promptflow._utils.multimedia_utils.ImageProcessor.is_url", return_value=True)
        mocker.patch("promptflow._utils.multimedia_utils.ImageProcessor.is_base64", return_value=False)
        mocker.patch("requests.get", return_value=mocker.Mock(content=image_from_path, status_code=200))
        image_from_url = self.processor.create_image("Test")
        assert str(image_from_path) == str(image_from_url)
        assert image_from_url._mime_type == "image/jpeg"

        ## From image
        image_from_image = self.processor.create_image(image_from_path)
        assert str(image_from_path) == str(image_from_image)

    def test_create_image_with_invalid_cases(self):
        # Test invalid input type
        with pytest.raises(InvalidImageInput) as ex:
            self.processor.create_image(0)
        assert "Unsupported image input type" in ex.value.message_format

        # Test invalid image dict
        with pytest.raises(InvalidImageInput) as ex:
            invalid_image_dict = {"invalid_image": "invalid_image"}
            self.processor.create_image(invalid_image_dict)
        assert "Invalid image input format" in ex.value.message_format

        # Test none or empty input value
        with pytest.raises(InvalidImageInput) as ex:
            self.processor.create_image(None)
        assert "Unsupported image input type" in ex.value.message_format

        with pytest.raises(InvalidImageInput) as ex:
            self.processor.create_image("")
        assert "The image input should not be empty." in ex.value.message_format

    def test_default_json_encoder(self):
        obj = PFBytes(b"test", "image/jpg", None)
        result = self.processor.default_json_encoder(obj)
        assert result == "b'test'"

        obj = Text("test")
        result = self.processor.default_json_encoder(obj)
        assert result == "test"

        class InvalidClass(str):
            def __new__(cls, value: str):
                obj = str.__new__(cls, value)
                return obj

        obj = InvalidClass("test")
        with pytest.raises(TypeError) as e:
            result = self.processor.default_json_encoder(obj)
        assert "Object of type InvalidClass is not JSON serializable" in str(e.value)

    def test_load_multimedia_data(self):
        # Case 1: Test normal node
        inputs = {
            "image": FlowInputDefinition(type=ValueType.IMAGE),
            "images": FlowInputDefinition(type=ValueType.LIST),
            "object": FlowInputDefinition(type=ValueType.OBJECT),
        }
        image_dict = {"type": "image_file", "image_file": {"path": str(TEST_IMAGE_PATH)}}
        line_inputs = {
            "image": image_dict,
            "images": [image_dict, image_dict],
            "object": {"image": image_dict, "other_data": "other_data"},
        }
        updated_inputs = self.processor.load_multimedia_data(inputs, line_inputs)
        image = ImageProcessor.create_image_from_file(TEST_IMAGE_PATH)
        assert updated_inputs == {
            "image": image,
            "images": [image, image],
            "object": {"image": image, "other_data": "other_data"},
        }

        # Case 2: Test aggregation node
        line_inputs = {
            "image": [image_dict, image_dict],
            "images": [[image_dict, image_dict], [image_dict]],
            "object": [{"image": image_dict, "other_data": "other_data"}, {"other_data": "other_data"}],
        }
        updated_inputs = self.processor.load_multimedia_data(inputs, line_inputs)
        assert updated_inputs == {
            "image": [image, image],
            "images": [[image, image], [image]],
            "object": [{"image": image, "other_data": "other_data"}, {"other_data": "other_data"}],
        }

        # Case 3: Test invalid input type
        with pytest.raises(LoadMultimediaDataError) as ex:
            line_inputs = {"image": 0}
            self.processor.load_multimedia_data(inputs, line_inputs)
        assert (
            "Failed to load image for input 'image': " "(InvalidImageInput) Unsupported image input type"
        ) in ex.value.message

    def test_resolve_multimedia_data_recursively(self):
        image_dict = {"type": "image_file", "image_file": {"path": "logo.jpg"}}
        value = {
            "image": image_dict,
            "images": [image_dict, image_dict],
            "object": {"image": image_dict, "other_data": "other_data"},
        }
        input_dir = TEST_IMAGE_PATH
        updated_value = self.processor.resolve_multimedia_data_recursively(input_dir, value)
        updated_image_dict = {"type": "image_file", "image_file": {"path": str(DATA_ROOT / "logo.jpg")}}
        assert updated_value == {
            "image": updated_image_dict,
            "images": [updated_image_dict, updated_image_dict],
            "object": {"image": updated_image_dict, "other_data": "other_data"},
        }

    def test_persist_multimedia_date(self, mocker):
        image = ImageProcessor.create_image_from_file(TEST_IMAGE_PATH)
        mocker.patch("builtins.open", mock_open())
        data = {"image": image, "images": [image, image, "other_data"], "other_data": "other_data"}
        persisted_data = self.processor.persist_multimedia_data(data, base_dir=Path(__file__).parent)

        file_name = re.compile(r"^[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}.jpeg$")

        def check_persisted_image_file(data: dict):
            assert data["type"] == "image_file"
            assert re.match(file_name, data["image_file"]["path"])

        check_persisted_image_file(persisted_data["image"])
        check_persisted_image_file(persisted_data["images"][0])
        check_persisted_image_file(persisted_data["images"][1])

    def test_convert_multimedia_date_to_base64(self):
        image = ImageProcessor.create_image_from_file(TEST_IMAGE_PATH)
        data = {"image": image, "images": [image, image, "other_data"], "other_data": "other_data"}
        base64_data = self.processor.convert_multimedia_data_to_base64_dict(data)
        excepted_image = {
            "type": "image_url",
            "image_url": {"url": f"data:{image._mime_type};base64,{image.to_base64()}"},
        }
        assert base64_data == {
            "image": excepted_image,
            "images": [excepted_image, excepted_image, "other_data"],
            "other_data": "other_data",
        }


@pytest.mark.unittest
def test_process_recursively():
    image = ImageProcessor.create_image_from_file(TEST_IMAGE_PATH)
    value = {"image": image, "images": [image, image], "object": {"image": image, "other_data": "other_data"}}
    process_funcs = {Image: lambda x: str(x)}
    updated_value = _process_recursively(value, process_funcs)
    image_str = str(image)
    assert updated_value == {
        "image": image_str,
        "images": [image_str, image_str],
        "object": {"image": image_str, "other_data": "other_data"},
    }
    assert value != updated_value


@pytest.mark.unittest
def test_process_recursively_inplace():
    image = ImageProcessor.create_image_from_file(TEST_IMAGE_PATH)
    value = {"image": image, "images": [image, image], "object": {"image": image, "other_data": "other_data"}}
    process_funcs = {Image: lambda x: str(x)}
    _process_recursively(value, process_funcs, inplace=True)
    image_str = str(image)
    assert value == {
        "image": image_str,
        "images": [image_str, image_str],
        "object": {"image": image_str, "other_data": "other_data"},
    }
