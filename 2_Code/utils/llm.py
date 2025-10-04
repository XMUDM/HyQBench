from transformers import AutoTokenizer, AutoModelForCausalLM
from openai import OpenAI
from google import genai

class GPT:
    def __init__(self, model="gpt-4.1"):
        self.model = model
        self.model_name = model

    def generate(self, prompt: str) -> str:
        client = OpenAI()
    
        completion = client.chat.completions.create(
            model = self.model,
            messages = [
                {
                    "role": "user",
                    "content":prompt
                }
            ],
            temperature=0.01
        )

        try:
            res = str(completion.choices[0].message.content)
            return res
        except:
            print('Error: ' + str(completion))
            return ""

    def __call__(self, prompt: str) -> str:
        return self.generate(prompt)
    
    def __str__(self):
        return self.model_name
    
class Gemini:
    def __init__(self, model="gemini-2.5-flash"):
        self.model = model
        self.model_name = model

    def generate(self, prompt: str) -> str:
        client = genai.Client()

        response = client.models.generate_content(
            model=self.model,
            contents=prompt,
        )

        return response.text

    def __call__(self, prompt: str) -> str:
        return self.generate(prompt)
    
    def __str__(self):
        return self.model_name
    
class QwenApi:
    def __init__(self, model="qwen-max-latest"):
        self.model = model
        self.model_name = "Qwen-Max"
        self.api_key=''


    def generate(self, prompt: str) -> str:
        client = OpenAI(
            api_key=self.api_key,
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )

        completion = client.chat.completions.create(
            model = self.model,
            messages = [
                {
                    "role": "user",
                    "content":prompt
                }
            ],
            temperature=0.01,
        )

        try:
            res = str(completion.choices[0].message.content)
            return res
        except:
            print('Error: ' + str(completion))
            return ""

    def __call__(self, prompt: str) -> str:
        return self.generate(prompt)
    
    def __str__(self):
        return self.model_name

class DP:
    def __init__(self, model="deepseek-chat"):
        self.model = model
        self.model_name = "DeepSeek-V3"
        self.api_key = ""

    def generate(self, prompt: str) -> str:
        client = OpenAI(api_key=self.api_key, base_url="https://api.deepseek.com")
        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "user", "content": prompt},
            ],
            stream=False,
        )

        try:
            res = str(response.choices[0].message.content)
            return res
        except:
            print('Error: ' + str(response))
            return ""

    def __call__(self, prompt: str) -> str:
        return self.generate(prompt)
    
    def __str__(self):
        return self.model_name

class Qwen3:
    def __init__(self, model_name):
        self.model_name = model_name
        if model_name == "Qwen3-4B":
            self.tokenizer = AutoTokenizer.from_pretrained('Qwen/Qwen3-4B')
            self.model = AutoModelForCausalLM.from_pretrained('Qwen/Qwen3-4B', torch_dtype="auto", device_map="auto")
        elif model_name == "Qwen3-8B":
            self.tokenizer = AutoTokenizer.from_pretrained('Qwen/Qwen3-8B')
            self.model = AutoModelForCausalLM.from_pretrained('Qwen/Qwen3-8B', torch_dtype="auto", device_map="auto")

    def generate(self, prompt: str) -> str:
        # prepare the model input
        messages = [
            {"role": "user", "content": prompt}
        ]
        text = self.tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
            enable_thinking=False # Switches between thinking and non-thinking modes. Default is True.
        )

        model_inputs = self.tokenizer([text], return_tensors="pt").to(self.model.device)

        generated_ids = self.model.generate(
            **model_inputs,
            max_new_tokens=512,
            temperature=0.01,
        )
        output_ids = generated_ids[0][len(model_inputs.input_ids[0]):].tolist() 
        content = self.tokenizer.decode(output_ids, skip_special_tokens=True)

        return content

    def __call__(self, prompt: str) -> str:
        return self.generate(prompt)
    
    def __str__(self):
        return self.model_name
