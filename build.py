import subprocess

def main():
    # Run the build command
    result = subprocess.run(['echo', 'Building the project...'], capture_output=True, text=True)
    
    # Print the output
    print(result.stdout)

if __name__ == "__main__":
    main()