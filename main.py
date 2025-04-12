# main.py
import yaml

def main():
    data = {'hello': 'world'}
    print(yaml.dump(data))

if __name__ == "__main__":
    main()